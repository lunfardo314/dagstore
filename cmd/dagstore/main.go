// Command dagstore serves a dagstore database: the query API, the dashboard and the DAG explorer.
// For now it serves a database read-only, as the offline importer leaves it; intake from feeders,
// the history walker and the sealer join this process later.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/api"
	"github.com/lunfardo314/dagstore/store"
)

func main() {
	cfg := api.DefaultConfig
	flag.StringVar(&cfg.DBPath, "db", "", "dagstore database file")
	listen := flag.String("listen", "127.0.0.1:8090", "address of the query API and the dashboard")
	conns := flag.Int("conns", 8, "read connections to the database")
	flag.DurationVar(&cfg.QueryTimeout, "query_timeout", cfg.QueryTimeout, "limit on the queries of one request")
	flag.DurationVar(&cfg.StatsInterval, "stats_interval", cfg.StatsInterval, "period of the stats snapshot")
	version := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *version {
		fmt.Println(dagstore.BannerString())
		return
	}
	log.Print(dagstore.BannerString())
	if cfg.DBPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	db, err := store.OpenReadOnly(cfg.DBPath, *conns)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	libs, err := api.LoadLedger(db)
	if err != nil {
		log.Fatalf("libraries: %v", err)
	}
	libInfo := api.CheckLibraries(libs)
	for _, l := range libInfo {
		log.Printf("library %d, slot %d: %s", l.UpgradeIndex, l.ActivationSlot, l.Check)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := api.New(db, cfg)
	go srv.RunStats(ctx, libInfo)

	httpSrv := &http.Server{Addr: *listen, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()
	log.Printf("serving %s on http://%s", cfg.DBPath, *listen)
	if err = httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
