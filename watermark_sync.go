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

package watch

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	syncHeartbeatInterval     = 60 * time.Second
	syncIdleLogInterval       = 60 * time.Second
	defaultSyncLookbackWindow = 48 * time.Hour
	syncStartTimeEnv          = "SYNC_TRANSACTION_START_TIME"
	syncLookbackWindowEnv     = "SYNC_TRANSACTION_LOOKBACK"
	syncTimestampLayout       = "2006-01-02 15:04:05"
	syncDateLayout            = "2006-01-02"
)

var initialSyncEpoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

type SyncWatermark struct {
	ID                  int       `json:"id"`
	LastSyncTimestamp   time.Time `json:"last_sync_timestamp"`
	LastTransactionID   string    `json:"last_transaction_id,omitempty"`
	TotalSyncedCount    int64     `json:"total_synced_count"`
	LastSyncCompletedAt time.Time `json:"last_sync_completed_at"`
	SyncStatus          string    `json:"sync_status"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type SyncConfig struct {
	SyncInterval         time.Duration // How often to run sync
	BatchSize            int           // Number of transactions to sync per batch
	MaxRetries           int           // Maximum number of retries on failure
	RetryDelay           time.Duration // Delay between retries
	EnableSync           bool          // Whether sync is enabled
	TransactionStartTime time.Time     // Starting timestamp for transaction sync
}

func DefaultSyncConfig() *SyncConfig {
	return &SyncConfig{
		SyncInterval:         10 * time.Second,
		BatchSize:            1000,
		MaxRetries:           3,
		RetryDelay:           30 * time.Second,
		EnableSync:           true,
		TransactionStartTime: resolveDefaultTransactionStartTime(time.Now().UTC()),
	}
}

type WatermarkSyncer struct {
	config        *SyncConfig
	stopChan      chan struct{}
	running       bool
	lastIdleLogAt time.Time
}

func NewWatermarkSyncer(config *SyncConfig) *WatermarkSyncer {
	if config == nil {
		config = DefaultSyncConfig()
	}
	return &WatermarkSyncer{
		config:   config,
		stopChan: make(chan struct{}),
	}
}

func (ws *WatermarkSyncer) Start() error {
	if !ws.config.EnableSync {
		log.Info().Msg("Watermark sync is disabled")
		return nil
	}

	if ws.running {
		return fmt.Errorf("watermark syncer is already running")
	}

	ws.running = true
	log.Info().
		Dur("interval", ws.config.SyncInterval).
		Int("batch_size", ws.config.BatchSize).
		Msg("Starting watermark syncer")

	go ws.syncLoop()
	return nil
}

func (ws *WatermarkSyncer) Stop() {
	if !ws.running {
		return
	}

	log.Info().Msg("Stopping watermark syncer")
	close(ws.stopChan)
	ws.running = false
}

func (ws *WatermarkSyncer) syncLoop() {
	ticker := time.NewTicker(ws.config.SyncInterval)
	defer ticker.Stop()

	if err := ws.performSync(); err != nil {
		log.Error().Err(err).Msg("Initial sync failed")
	}

	for {
		select {
		case <-ticker.C:
			if err := ws.performSync(); err != nil {
				log.Error().Err(err).Msg("Periodic sync failed")
			}
		case <-ws.stopChan:
			log.Info().Msg("Watermark sync loop stopped")
			return
		}
	}
}

func (ws *WatermarkSyncer) performSync() error {
	var lastErr error

	for attempt := 1; attempt <= ws.config.MaxRetries; attempt++ {
		err := ws.syncTransactionsIncremental()
		if err == nil {
			return nil
		}

		lastErr = err
		log.Warn().
			Err(err).
			Int("attempt", attempt).
			Int("max_retries", ws.config.MaxRetries).
			Msg("Sync attempt failed, retrying")

		if attempt < ws.config.MaxRetries {
			time.Sleep(ws.config.RetryDelay)
		}
	}

	return fmt.Errorf("sync failed after %d attempts: %w", ws.config.MaxRetries, lastErr)
}

func attachPostgresDB(db *sql.DB, dbURL string) error {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return fmt.Errorf("invalid database URL: %w", err)
	}

	attachStmt := fmt.Sprintf("ATTACH '%s' AS postgres_db (TYPE postgres);", parsedURL.String())
	_, err = db.Exec(attachStmt)

	return err
}

func startSyncHeartbeat(watermark *SyncWatermark) func() {
	done := make(chan struct{})
	startedAt := time.Now()
	ticker := time.NewTicker(syncHeartbeatInterval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				log.Info().
					Dur("elapsed", time.Since(startedAt).Round(time.Second)).
					Time("last_sync_timestamp", watermark.LastSyncTimestamp).
					Int64("total_synced", watermark.TotalSyncedCount).
					Msg("Watermark sync still running")
			case <-done:
				return
			}
		}
	}()

	return func() {
		close(done)
	}
}

func resolveDefaultTransactionStartTime(now time.Time) time.Time {
	if raw := strings.TrimSpace(os.Getenv(syncStartTimeEnv)); raw != "" {
		parsed, err := parseSyncStartTime(raw)
		if err != nil {
			log.Warn().Err(err).Str("env", syncStartTimeEnv).Str("value", raw).Msg("Invalid sync start time override, falling back to lookback window")
		} else {
			return parsed.UTC()
		}
	}

	lookback := defaultSyncLookbackWindow
	if raw := strings.TrimSpace(os.Getenv(syncLookbackWindowEnv)); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			log.Warn().Err(err).Str("env", syncLookbackWindowEnv).Str("value", raw).Msg("Invalid sync lookback override, using default")
		} else {
			lookback = parsed
		}
	}

	return now.Add(-lookback).UTC()
}

func parseSyncStartTime(raw string) (time.Time, error) {
	layouts := []string{
		time.RFC3339,
		syncTimestampLayout,
		syncDateLayout,
	}

	var lastErr error
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed, nil
		}
		lastErr = err
	}

	return time.Time{}, fmt.Errorf("parse %q using RFC3339, %s, or %s: %w", raw, syncTimestampLayout, syncDateLayout, lastErr)
}

func defaultInitialSyncTimestamp() string {
	return resolveDefaultTransactionStartTime(time.Now().UTC()).Format(syncTimestampLayout)
}

func (ws *WatermarkSyncer) normalizeInitialWatermark(watermark *SyncWatermark) {
	if watermark.TotalSyncedCount != 0 {
		return
	}
	if watermark.LastTransactionID != "" {
		return
	}
	if !watermark.LastSyncTimestamp.Equal(initialSyncEpoch) {
		return
	}

	watermark.LastSyncTimestamp = ws.config.TransactionStartTime.UTC()
}

func (ws *WatermarkSyncer) syncTransactionsIncremental() error {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		return fmt.Errorf("DB_URL environment variable is required")
	}

	db, err := GetSyncDB()
	if err != nil {
		return fmt.Errorf("failed to get DuckDB connection: %w", err)
	}

	watermark, err := ws.getWatermark(db)
	if err != nil {
		return fmt.Errorf("failed to get watermark: %w", err)
	}

	if err := ws.updateSyncStatus(db, "running"); err != nil {
		log.Warn().Err(err).Msg("Failed to update sync status to running")
	}

	syncStartedAt := time.Now()
	log.Debug().
		Int("batch_size", ws.config.BatchSize).
		Time("last_sync_timestamp", watermark.LastSyncTimestamp).
		Str("last_transaction_id", watermark.LastTransactionID).
		Msg("Watermark sync cycle started")
	stopHeartbeat := startSyncHeartbeat(watermark)
	defer stopHeartbeat()

	_, err = db.Exec("INSTALL postgres; LOAD postgres;")
	if err != nil {
		return fmt.Errorf("failed to install/load PostgreSQL extension: %w", err)
	}

	err = attachPostgresDB(db, dbURL)
	if err != nil {
		return fmt.Errorf("failed to attach PostgreSQL database: %w", err)
	}

	defer func() {
		if _, err := db.Exec("DETACH postgres_db;"); err != nil {
			log.Error().Err(err).Msg("Failed to detach PostgreSQL database")
		}
	}()

	syncResults, err := ws.performIncrementalCopy(db, watermark)
	if err != nil {
		if updateErr := ws.updateSyncStatus(db, "failed"); updateErr != nil {
			log.Warn().Err(updateErr).Msg("Failed to update sync status to failed")
		}
		return fmt.Errorf("failed to perform incremental copy: %w", err)
	}

	if err := ws.updateWatermarkFull(db, syncResults); err != nil {
		log.Error().Err(err).Msg("Failed to update watermark, but sync completed")
	}

	if err := ws.updateSyncStatus(db, "idle"); err != nil {
		log.Warn().Err(err).Msg("Failed to update sync status to idle")
	}

	if !syncResults.HasBatch {
		if ws.shouldLogIdleCycle(syncStartedAt) {
			log.Info().
				Dur("elapsed", time.Since(syncStartedAt).Round(time.Millisecond)).
				Time("last_sync_timestamp", watermark.LastSyncTimestamp).
				Int64("total_synced", watermark.TotalSyncedCount).
				Msg("Watermark sync idle")
		}
		return nil
	}

	ws.lastIdleLogAt = time.Time{}
	log.Info().
		Dur("elapsed", time.Since(syncStartedAt).Round(time.Millisecond)).
		Int64("transactions_synced", syncResults.TransactionsSynced).
		Int64("total_synced", watermark.TotalSyncedCount+syncResults.TransactionsSynced).
		Time("new_watermark", syncResults.TransactionWatermark).
		Str("last_transaction_id", syncResults.LastTransactionID).
		Msg("Watermark sync cycle completed")

	return nil
}

type SyncResult struct {
	TransactionWatermark time.Time
	LastTransactionID    string
	TransactionsSynced   int64
	HasBatch             bool
}

func (ws *WatermarkSyncer) performIncrementalCopy(db *sql.DB, watermark *SyncWatermark) (*SyncResult, error) {
	boundary, err := ws.getBatchBoundary(db, watermark)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch boundary: %w", err)
	}

	result := &SyncResult{
		TransactionWatermark: watermark.LastSyncTimestamp,
		LastTransactionID:    watermark.LastTransactionID,
		HasBatch:             boundary.HasBatch,
	}
	if !boundary.HasBatch {
		return result, nil
	}

	rowsAffected, err := ws.copyTransactions(db, watermark)
	if err != nil {
		return nil, fmt.Errorf("failed to copy transactions: %w", err)
	}

	result.TransactionWatermark = ws.calculateNewWatermark(watermark, boundary.MaxTimestamp, rowsAffected)
	result.LastTransactionID = boundary.LastTransactionID
	result.TransactionsSynced = rowsAffected

	return result, nil
}

type syncBatchBoundary struct {
	MaxTimestamp      sql.NullTime
	LastTransactionID string
	HasBatch          bool
}

func (ws *WatermarkSyncer) getBatchBoundary(db *sql.DB, watermark *SyncWatermark) (*syncBatchBoundary, error) {
	query := ws.buildBatchBoundaryQuery(watermark)

	var boundary syncBatchBoundary
	var lastTxnID sql.NullString
	err := db.QueryRow(query).Scan(&boundary.MaxTimestamp, &lastTxnID)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	boundary.HasBatch = boundary.MaxTimestamp.Valid
	if lastTxnID.Valid {
		boundary.LastTransactionID = lastTxnID.String
	}

	return &boundary, nil
}

func (ws *WatermarkSyncer) buildBatchBoundaryQuery(watermark *SyncWatermark) string {
	baseQuery := `
		WITH batch_transactions AS (
			SELECT pg_txn.transaction_id, pg_txn.created_at
			FROM postgres_db.blnk.transactions pg_txn
			WHERE pg_txn.status IS NOT NULL 
			  AND pg_txn.status != 'QUEUED'
			  %s
			  AND NOT EXISTS (
				  SELECT 1 FROM transactions local_txn 
				  WHERE local_txn.transaction_id = pg_txn.transaction_id
			  )
			ORDER BY pg_txn.created_at ASC, pg_txn.transaction_id ASC
			LIMIT %d
		)
		SELECT
			MAX(created_at) AS max_created_at,
			(
				SELECT transaction_id
				FROM batch_transactions
				ORDER BY created_at DESC, transaction_id DESC
				LIMIT 1
			) AS last_transaction_id
		FROM batch_transactions`

	whereClause := ws.buildTimestampWhereClause(watermark)
	return fmt.Sprintf(baseQuery, whereClause, ws.config.BatchSize)
}

func (ws *WatermarkSyncer) copyTransactions(db *sql.DB, watermark *SyncWatermark) (int64, error) {
	query := ws.buildCopyQuery(watermark)

	result, err := db.Exec(query)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Warn().Err(err).Msg("Could not determine number of rows copied")
		return 0, nil // Return 0 but no error since the operation succeeded
	}

	return rowsAffected, nil
}

func (ws *WatermarkSyncer) buildCopyQuery(watermark *SyncWatermark) string {
	baseQuery := `
		INSERT OR REPLACE INTO transactions (transaction_id, amount, currency, source, destination, timestamp, description, metadata)
		SELECT 
			pg_txn.transaction_id as transaction_id,
			COALESCE(pg_txn.amount, 0) as amount,
			COALESCE(pg_txn.currency, '') as currency,
			COALESCE(pg_txn.source, '') as source,
			COALESCE(pg_txn.destination, '') as destination,
			COALESCE(pg_txn.created_at, NOW()) as timestamp,
			COALESCE(pg_txn.description, '') as description,
			COALESCE(pg_txn.meta_data, '{}') as metadata
		FROM postgres_db.blnk.transactions pg_txn
		WHERE pg_txn.status IS NOT NULL 
		  AND pg_txn.status != 'QUEUED'
		  %s
		  AND NOT EXISTS (
			  SELECT 1 FROM transactions local_txn 
			  WHERE local_txn.transaction_id = pg_txn.transaction_id
		  )
		ORDER BY pg_txn.created_at ASC, pg_txn.transaction_id ASC
		LIMIT %d`

	whereClause := ws.buildTimestampWhereClause(watermark)
	return fmt.Sprintf(baseQuery, whereClause, ws.config.BatchSize)
}

func (ws *WatermarkSyncer) buildTimestampWhereClause(watermark *SyncWatermark) string {
	timestampStr := watermark.LastSyncTimestamp.Format(syncTimestampLayout)

	if watermark.LastTransactionID != "" {
		return fmt.Sprintf(
			"AND (pg_txn.created_at > '%s' OR (pg_txn.created_at = '%s' AND pg_txn.transaction_id > '%s'))",
			timestampStr, timestampStr, watermark.LastTransactionID,
		)
	}

	return fmt.Sprintf("AND pg_txn.created_at > '%s'", timestampStr)
}

func (ws *WatermarkSyncer) calculateNewWatermark(watermark *SyncWatermark, maxTimestamp sql.NullTime, rowsAffected int64) time.Time {
	if rowsAffected == 0 {
		// No new transactions, keep the same watermark
		return watermark.LastSyncTimestamp
	}

	if maxTimestamp.Valid {
		// Use the max timestamp from the batch we just copied
		return maxTimestamp.Time
	}

	// Fallback: if we copied rows but couldn't get max timestamp, use current time
	log.Warn().Msg("Copied transactions but couldn't determine max timestamp, using current time")
	return time.Now()
}

func (ws *WatermarkSyncer) getWatermark(db *sql.DB) (*SyncWatermark, error) {
	transactionDefault := ws.config.TransactionStartTime.Format("2006-01-02 15:04:05")

	query := fmt.Sprintf(`
		SELECT
			id,
			last_sync_timestamp,
			COALESCE(last_transaction_id, ''),
			total_synced_count,
			COALESCE(last_sync_completed_at, '%s'),
			sync_status,
			created_at,
			updated_at
		FROM sync_watermark
		WHERE id = 1
	`, transactionDefault)

	var w SyncWatermark
	err := db.QueryRow(query).Scan(
		&w.ID,
		&w.LastSyncTimestamp,
		&w.LastTransactionID,
		&w.TotalSyncedCount,
		&w.LastSyncCompletedAt,
		&w.SyncStatus,
		&w.CreatedAt,
		&w.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get watermark: %w", err)
	}

	ws.normalizeInitialWatermark(&w)
	return &w, nil
}

func (ws *WatermarkSyncer) updateWatermarkFull(db *sql.DB, result *SyncResult) error {
	transactionWatermark := result.TransactionWatermark
	if transactionWatermark.IsZero() || transactionWatermark.Before(ws.config.TransactionStartTime) {
		transactionWatermark = ws.config.TransactionStartTime
	}

	query := `
		UPDATE sync_watermark
		SET
			last_sync_timestamp = ?,
			last_transaction_id = ?,
			total_synced_count = total_synced_count + ?,
			last_sync_completed_at = CURRENT_TIMESTAMP,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = 1
	`

	_, err := db.Exec(query,
		transactionWatermark.Format(syncTimestampLayout),
		result.LastTransactionID,
		result.TransactionsSynced,
	)
	if err != nil {
		return fmt.Errorf("failed to update watermark: %w", err)
	}

	return nil
}

func (ws *WatermarkSyncer) updateSyncStatus(db *sql.DB, status string) error {
	query := `
		UPDATE sync_watermark 
		SET sync_status = ?,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = 1
	`

	_, err := db.Exec(query, status)
	if err != nil {
		return fmt.Errorf("failed to update sync status: %w", err)
	}

	return nil
}

func (ws *WatermarkSyncer) GetSyncStatus() (*SyncWatermark, error) {
	db, err := GetSyncDB()
	if err != nil {
		return nil, fmt.Errorf("failed to get DuckDB connection: %w", err)
	}

	watermark, err := ws.getWatermark(db)
	if err != nil {
		return nil, err
	}

	log.Debug().
		Int64("transactions_synced", watermark.TotalSyncedCount).
		Time("last_transaction_sync", watermark.LastSyncTimestamp).
		Str("status", watermark.SyncStatus).
		Msg("Current sync status")

	return watermark, nil
}

func (ws *WatermarkSyncer) ForceSync() error {
	log.Info().Msg("Force sync triggered")
	return ws.performSync()
}

func (ws *WatermarkSyncer) ResetWatermark() error {
	db, err := GetSyncDB()
	if err != nil {
		return fmt.Errorf("failed to get DuckDB connection: %w", err)
	}

	transactionStart := ws.config.TransactionStartTime.Format(syncTimestampLayout)

	query := fmt.Sprintf(`
		UPDATE sync_watermark
		SET
			last_sync_timestamp = '%s',
			last_transaction_id = NULL,
			total_synced_count = 0,
			sync_status = 'idle',
			updated_at = CURRENT_TIMESTAMP
		WHERE id = 1
	`, transactionStart)

	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to reset watermark: %w", err)
	}

	log.Info().Msg("Watermark reset successfully")
	return nil
}

func (ws *WatermarkSyncer) shouldLogIdleCycle(now time.Time) bool {
	if ws.lastIdleLogAt.IsZero() || now.Sub(ws.lastIdleLogAt) >= syncIdleLogInterval {
		ws.lastIdleLogAt = now
		return true
	}

	return false
}
