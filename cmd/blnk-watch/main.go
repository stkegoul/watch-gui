/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"watch"
)

func main() {
	var (
		command      = flag.String("command", "watch", "Command to run: 'watch' (default), 'sync', or 'sync-once'")
		envFile      = flag.String("env", ".env", "Path to .env file")
		port         = flag.String("port", "8081", "Port for watch service HTTP server")
		syncInterval = flag.Duration("sync-interval", 1*time.Second, "Interval for watermark sync")
		batchSize    = flag.Int("batch-size", 1000, "Batch size for watermark sync")
	)
	flag.Parse()

	if err := godotenv.Load(*envFile); err != nil {
		zlog.Warn().Err(err).Msg("Failed to load .env file, using environment variables")
	}

	zlog.Logger = zlog.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	switch *command {
	case "watch":
		runWatchService(*port)
	case "sync":
		runWatermarkSync(*syncInterval, *batchSize)
	case "sync-once":
		runWatermarkSyncOnce(*batchSize)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", *command)
		fmt.Fprintf(os.Stderr, "Available commands: watch, sync, sync-once\n")
		os.Exit(1)
	}
}

func runWatchService(port string) {
	zlog.Info().Msg("Starting Blnk Watch Service...")

	os.Setenv("PORT", port)
	if port == "" {
		port = "8081"
	}

	watch.SetupWatchService(nil)
}

func runWatermarkSync(syncInterval time.Duration, batchSize int) {
	zlog.Info().Msg("Starting Watermark Sync Service...")

	if err := watch.InitTransactionsDB(); err != nil {
		zlog.Fatal().Err(err).Msg("Failed to initialize transactions database")
	}

	config := watch.DefaultSyncConfig()
	config.SyncInterval = syncInterval
	config.BatchSize = batchSize

	syncer := watch.NewWatermarkSyncer(config)

	if err := syncer.Start(); err != nil {
		zlog.Fatal().Err(err).Msg("Failed to start watermark syncer")
	}

	zlog.Info().
		Dur("interval", syncInterval).
		Int("batch_size", batchSize).
		Msg("Watermark sync service running")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	zlog.Info().Msg("Shutting down watermark sync service...")
	syncer.Stop()
	zlog.Info().Msg("Watermark sync service stopped")
}

func runWatermarkSyncOnce(batchSize int) {
	zlog.Info().Msg("Running one-time watermark sync...")

	if err := watch.InitTransactionsDB(); err != nil {
		zlog.Fatal().Err(err).Msg("Failed to initialize transactions database")
	}

	config := watch.DefaultSyncConfig()
	config.BatchSize = batchSize
	config.EnableSync = true

	syncer := watch.NewWatermarkSyncer(config)

	if err := syncer.ForceSync(); err != nil {
		zlog.Fatal().Err(err).Msg("Failed to perform watermark sync")
	}

	status, err := syncer.GetSyncStatus()
	if err != nil {
		zlog.Warn().Err(err).Msg("Failed to get sync status")
	} else {
		zlog.Info().
			Int64("transactions_synced", status.TotalSyncedCount).
			Msg("Sync completed successfully")
	}

	zlog.Info().Msg("One-time sync completed")
}
