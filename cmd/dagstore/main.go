// Command dagstore serves a dagstore database: the query API, the dashboard and the DAG explorer.
// For now it serves a database read-only, as the offline importer leaves it; intake from feeders,
// the history walker and the sealer join this process later.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
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
	flag.DurationVar(&cfg.SlowRequest, "slow_request", cfg.SlowRequest, "requests taking longer are logged")
	logLevel := flag.String("log_level", "info", "debug, info, warn or error; debug logs every request")
	version := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *version {
		fmt.Println(dagstore.BannerString())
		return
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "bad -log_level: %v\n", err)
		os.Exit(2)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	slog.Info(dagstore.BannerString())
	if cfg.DBPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	db, err := store.OpenReadOnly(cfg.DBPath, *conns)
	if err != nil {
		fatal("open database", err)
	}
	defer db.Close()
	libs, err := api.LoadLedger(db)
	if err != nil {
		fatal("load libraries", err)
	}
	libInfo := api.CheckLibraries(libs)
	matching := 0
	for _, l := range libInfo {
		if l.Check == "genesis" || l.Check == "matches this build" {
			matching++
			slog.Debug("library", "upgrade", l.UpgradeIndex, "slot", l.ActivationSlot, "check", l.Check)
		} else {
			slog.Warn("library", "upgrade", l.UpgradeIndex, "slot", l.ActivationSlot, "check", l.Check)
		}
	}
	slog.Info("database", "path", cfg.DBPath, "libraries", len(libs), "matching_build", matching)

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
	slog.Info("listening", "url", "http://"+*listen)
	if err = httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fatal("serve", err)
	}
	slog.Info("stopped")
}

func fatal(what string, err error) {
	slog.Error(what, "err", err)
	os.Exit(1)
}
