package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"top-queries/internal/app"
	"top-queries/internal/config"
	"top-queries/internal/logger"

	"go.uber.org/zap"
)

func main() {
	configFilePath := os.Getenv("CONFIG_PATH")
	if configFilePath == "" {
		fmt.Fprintln(os.Stderr, "critical error: env CONFIG_PATH is empty")
		os.Exit(1)
	}

	cfg, err := config.Load(configFilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "critical error on loading config: %v\n", err)
		os.Exit(1)
	}

	log := logger.NewLogger(cfg.App.LogLevel, cfg.App.IsProd)
	defer func() {
		_ = log.Sync()
	}()

	queryApp, err := app.New(cfg, log)
	if err != nil {
		log.Error("failed to initialize application layers", zap.Error(err))
		return
	}

	log.Info("starting top-queries application...")
	if err := queryApp.Run(); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Info("application execution was canceled by context")
		} else {
			log.Error("application exited with critical error", zap.Error(err))
			os.Exit(1)
		}
	}
}
