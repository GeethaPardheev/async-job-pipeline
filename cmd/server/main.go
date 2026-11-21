// Command server runs the job pipeline in api, worker or migrate mode (argv[1] wins over MODE).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/GeethaPardheev/async-job-pipeline/internal/config"
	"github.com/GeethaPardheev/async-job-pipeline/internal/controlplane"
	"github.com/GeethaPardheev/async-job-pipeline/internal/dataplane"
	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
)

var version = "dev"

func main() { os.Exit(run()) }

func run() int {
	boot := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("version", version)

	modeArg := os.Getenv("MODE")
	if len(os.Args) > 1 {
		modeArg = os.Args[1]
	}
	mode, err := config.ParseMode(modeArg)
	if err != nil {
		boot.Error("startup failed", "err", err)
		return 2
	}
	cfg, err := config.Load(mode, os.Getenv)
	if err != nil {
		boot.Error("startup failed", "mode", mode, "err", err)
		return 1
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With("mode", string(mode), "version", version)
	if mode == config.ModeWorker {
		log = log.With("worker_id", cfg.WorkerID)
	}
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	st, err := store.Open(ctx, store.Options{
		DatabaseURL:  cfg.DatabaseURL,
		Schema:       cfg.DBSchema,
		MaxConns:     cfg.DBMaxConns,
		QueryTimeout: cfg.DBQueryTimeout,
		AppName:      "jobq-" + string(mode),
	}, log)
	if err != nil {
		log.Error("startup failed", "err", err)
		return 1
	}
	defer st.Close()

	if mode == config.ModeMigrate || cfg.MigrateOnStart {
		if _, err := st.Migrate(ctx); err != nil {
			log.Error("migrations failed", "err", err)
			return 1
		}
	}

	switch mode {
	case config.ModeMigrate:
		return 0
	case config.ModeAPI:
		err = controlplane.New(controlplane.Config{Port: cfg.Port, APIToken: cfg.APIToken, Version: version}, st, log).Run(ctx)
	case config.ModeWorker:
		err = dataplane.New(dataplane.Config{WorkerID: cfg.WorkerID, Concurrency: cfg.WorkerConcurrency, Version: version}, log).Run(ctx)
	}
	if err != nil {
		log.Error("exited with error", "err", err)
		return 1
	}
	log.Info("shutdown complete")
	return 0
}
