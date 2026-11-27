// Command dagstore serves a dagstore database: the query API, the dashboard and the DAG explorer.
// For now it serves a database read-only, as the offline importer leaves it; intake from feeders,
// the history walker and the sealer join this process later.
package main

import (
	"context"
	"database/sql"
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
	"github.com/lunfardo314/dagstore/live"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/global"
	"github.com/spf13/viper"
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
	configPath := flag.String("config", "", "live mode: config file with the peering section (feeders); without it the database is served read-only")
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
	var libraryAPI string
	if *configPath != "" {
		viper.SetConfigFile(*configPath)
		if err := viper.ReadInConfig(); err != nil {
			fatal("config", err)
		}
		if v := viper.GetString("dagstore.db"); v != "" && cfg.DBPath == "" {
			cfg.DBPath = v
		}
		if v := viper.GetString("dagstore.listen"); v != "" {
			*listen = v
		}
		libraryAPI = viper.GetString("dagstore.library_api")
		// pull budgets are config values (SPEC.md section 6.3); the defaults spare production feeders
		if v := viper.GetInt("dagstore.pulls_per_second_per_feeder"); v > 0 {
			cfg.Params.PullsPerSecondPerFeeder = v
		}
		if v := viper.GetInt("dagstore.history_pulls_per_second"); v > 0 {
			cfg.Params.HistoryPullsPerSecond = v
		}
	}
	if cfg.DBPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	// live mode: the write connection and the ledger libraries come first, then the readers
	var writeDB *sql.DB
	var libs *api.Libraries
	var err error
	if *configPath != "" {
		if writeDB, libs, err = live.Open(cfg.DBPath, libraryAPI); err != nil {
			fatal("open database", err)
		}
		defer writeDB.Close()
	}
	db, err := store.OpenReadOnly(cfg.DBPath, *conns)
	if err != nil {
		fatal("open database", err)
	}
	defer db.Close()
	if writeDB == nil {
		if libs, err = api.LoadLedger(db); err != nil {
			fatal("load libraries", err)
		}
	}
	proven, pending := 0, 0
	for _, l := range libs.Info() {
		switch {
		case l.Alarm || (l.Proven && l.Check != "genesis" && l.Check != "matches this build"):
			slog.Warn("library", "upgrade", l.UpgradeIndex, "slot", l.ActivationSlot, "check", l.Check)
		default:
			slog.Debug("library", "upgrade", l.UpgradeIndex, "slot", l.ActivationSlot, "check", l.Check)
		}
		if l.Proven {
			proven++
		} else {
			pending++
		}
	}
	slog.Info("database", "path", cfg.DBPath, "libraries_proven", proven, "libraries_from_build", pending, "alarms", len(libs.Alarms()))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := api.New(db, cfg, libs)
	go srv.RunStats(ctx)
	if writeDB != nil {
		env := global.NewFromConfig()
		node, err := live.New(writeDB, db, env, cfg.Params, libs, libraryAPI)
		if err != nil {
			fatal("live", err)
		}
		srv.Live = node.Status
		srv.ExtraMetrics = env.MetricsRegistry()
		go func() {
			if err := node.Run(ctx); err != nil {
				fatal("live", err)
			}
			env.Stop()
		}()
	}

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
