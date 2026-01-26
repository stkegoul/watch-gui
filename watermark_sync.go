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
	"time"

	"github.com/rs/zerolog/log"
)

type SyncWatermark struct {
	ID int `json:"id"`
	// Transaction sync fields
	LastSyncTimestamp time.Time `json:"last_sync_timestamp"`
	LastTransactionID string    `json:"last_transaction_id,omitempty"`
	TotalSyncedCount  int64     `json:"total_synced_count"`
	// Identity sync fields
	LastIdentitySyncTimestamp time.Time `json:"last_identity_sync_timestamp"`
	LastIdentityID            string    `json:"last_identity_id,omitempty"`
	TotalIdentitiesSynced     int64     `json:"total_identities_synced"`
	// Balance sync fields
	LastBalanceSyncTimestamp time.Time `json:"last_balance_sync_timestamp"`
	LastBalanceID            string    `json:"last_balance_id,omitempty"`
	TotalBalancesSynced      int64     `json:"total_balances_synced"`
	// Ledger sync fields
	LastLedgerSyncTimestamp time.Time `json:"last_ledger_sync_timestamp"`
	LastLedgerID            string    `json:"last_ledger_id,omitempty"`
	TotalLedgersSynced      int64     `json:"total_ledgers_synced"`
	// General sync fields
	LastSyncCompletedAt time.Time `json:"last_sync_completed_at"`
	SyncStatus          string    `json:"sync_status"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type SyncConfig struct {
	SyncInterval time.Duration // How often to run sync
	BatchSize    int           // Number of transactions to sync per batch
	MaxRetries   int           // Maximum number of retries on failure
	RetryDelay   time.Duration // Delay between retries
	EnableSync   bool          // Whether sync is enabled
	// Custom starting timestamps for initial sync
	TransactionStartTime time.Time // Starting timestamp for transaction sync
	IdentityStartTime    time.Time // Starting timestamp for identity sync
	BalanceStartTime     time.Time // Starting timestamp for balance sync
	LedgerStartTime      time.Time
}

func DefaultSyncConfig() *SyncConfig {
	epochTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	return &SyncConfig{
		SyncInterval: 1 * time.Second,
		BatchSize:    1000,
		MaxRetries:   3,
		RetryDelay:   30 * time.Second,
		EnableSync:   true,
		TransactionStartTime: epochTime,
		IdentityStartTime:    epochTime,
		BalanceStartTime:     epochTime,
		LedgerStartTime:      epochTime,
	}
}

type WatermarkSyncer struct {
	config   *SyncConfig
	stopChan chan struct{}
	running  bool
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

func (ws *WatermarkSyncer) syncTransactionsIncremental() error {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		return fmt.Errorf("DB_URL environment variable is required")
	}

	db, err := GetDB()
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

	log.Info().
		Time("last_sync_timestamp", watermark.LastSyncTimestamp).
		Int64("total_synced", watermark.TotalSyncedCount).
		Str("last_transaction_id", watermark.LastTransactionID).
		Msg("Starting incremental sync")

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

	log.Info().
		Int64("transactions_synced", syncResults.TransactionsSynced).
		Int64("identities_synced", syncResults.IdentitiesSynced).
		Int64("balances_synced", syncResults.BalancesSynced).
		Int64("ledgers_synced", syncResults.LedgersSynced).
		Msg("Incremental sync completed successfully")

	return nil
}

type SyncResult struct {
	TransactionWatermark time.Time
	LastTransactionID    string
	TransactionsSynced   int64
	// Identity sync results
	IdentityWatermark time.Time
	LastIdentityID    string
	IdentitiesSynced  int64
	// Balance sync results
	BalanceWatermark time.Time
	LastBalanceID    string
	BalancesSynced   int64
	// Ledger sync results
	LedgerWatermark time.Time
	LastLedgerID    string
	LedgersSynced   int64
}

func (ws *WatermarkSyncer) performIncrementalCopy(db *sql.DB, watermark *SyncWatermark) (*SyncResult, error) {
	result := &SyncResult{}

	maxTimestamp, err := ws.getBatchMaxTimestamp(db, watermark)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch max timestamp: %w", err)
	}

	rowsAffected, err := ws.copyTransactions(db, watermark)
	if err != nil {
		return nil, fmt.Errorf("failed to copy transactions: %w", err)
	}

	lastTransactionID, err := ws.getLastTransactionID(db, watermark, rowsAffected)
	if err != nil {
		log.Warn().Err(err).Msg("Could not determine last transaction ID")
		lastTransactionID = watermark.LastTransactionID // fallback
	}

	result.TransactionWatermark = ws.calculateNewWatermark(watermark, maxTimestamp, rowsAffected)
	result.LastTransactionID = lastTransactionID
	result.TransactionsSynced = rowsAffected

	identitiesSynced, lastIdentityID, identityWatermark, err := ws.copyIdentities(db, watermark)
	if err != nil {
		log.Error().Err(err).Msg("Failed to sync identities")
		if watermark.LastIdentitySyncTimestamp.IsZero() {
			result.IdentityWatermark = ws.config.IdentityStartTime
		} else {
			result.IdentityWatermark = watermark.LastIdentitySyncTimestamp
		}
		result.LastIdentityID = watermark.LastIdentityID
	} else {
		result.IdentitiesSynced = identitiesSynced
		result.LastIdentityID = lastIdentityID
		result.IdentityWatermark = identityWatermark
	}

	// Sync balances
	balancesSynced, lastBalanceID, balanceWatermark, err := ws.copyBalances(db, watermark)
	if err != nil {
		log.Error().Err(err).Msg("Failed to sync balances")
		if watermark.LastBalanceSyncTimestamp.IsZero() {
			result.BalanceWatermark = ws.config.BalanceStartTime
		} else {
			result.BalanceWatermark = watermark.LastBalanceSyncTimestamp
		}
		result.LastBalanceID = watermark.LastBalanceID
	} else {
		result.BalancesSynced = balancesSynced
		result.LastBalanceID = lastBalanceID
		result.BalanceWatermark = balanceWatermark
	}

	// Sync ledgers
	ledgersSynced, lastLedgerID, ledgerWatermark, err := ws.copyLedgers(db, watermark)
	if err != nil {
		log.Error().Err(err).Msg("Failed to sync ledgers")
		if watermark.LastLedgerSyncTimestamp.IsZero() {
			result.LedgerWatermark = ws.config.LedgerStartTime
		} else {
			result.LedgerWatermark = watermark.LastLedgerSyncTimestamp
		}
		result.LastLedgerID = watermark.LastLedgerID
	} else {
		result.LedgersSynced = ledgersSynced
		result.LastLedgerID = lastLedgerID
		result.LedgerWatermark = ledgerWatermark
	}

	return result, nil
}

func (ws *WatermarkSyncer) getBatchMaxTimestamp(db *sql.DB, watermark *SyncWatermark) (sql.NullTime, error) {
	query := ws.buildMaxTimestampQuery(watermark)

	var maxTimestamp sql.NullTime
	err := db.QueryRow(query).Scan(&maxTimestamp)
	if err != nil && err != sql.ErrNoRows {
		return maxTimestamp, err
	}

	return maxTimestamp, nil
}

func (ws *WatermarkSyncer) buildMaxTimestampQuery(watermark *SyncWatermark) string {
	baseQuery := `
		SELECT MAX(created_at) 
		FROM (
			SELECT pg_txn.created_at
			FROM postgres_db.blnk.transactions pg_txn
			WHERE pg_txn.status IS NOT NULL 
			  AND pg_txn.status != 'QUEUED'
			  %s
			  AND NOT EXISTS (
				  SELECT 1 FROM transactions local_txn 
				  WHERE local_txn.transaction_id = pg_txn.transaction_id
			  )
			ORDER BY pg_txn.created_at ASC
			LIMIT %d
		) AS batch_transactions`

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
		ORDER BY pg_txn.created_at ASC
		LIMIT %d`

	whereClause := ws.buildTimestampWhereClause(watermark)
	return fmt.Sprintf(baseQuery, whereClause, ws.config.BatchSize)
}

func (ws *WatermarkSyncer) getLastTransactionID(db *sql.DB, watermark *SyncWatermark, rowsAffected int64) (string, error) {
	if rowsAffected == 0 {
		return watermark.LastTransactionID, nil
	}

	query := ws.buildLastTransactionQuery(watermark)

	var lastTxnID sql.NullString
	err := db.QueryRow(query).Scan(&lastTxnID)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}

	if lastTxnID.Valid {
		return lastTxnID.String, nil
	}

	return watermark.LastTransactionID, nil
}

func (ws *WatermarkSyncer) buildLastTransactionQuery(watermark *SyncWatermark) string {
	baseQuery := `
		SELECT pg_txn.transaction_id
		FROM postgres_db.blnk.transactions pg_txn
		WHERE pg_txn.status IS NOT NULL 
		  AND pg_txn.status != 'QUEUED'
		  %s
		  AND NOT EXISTS (
			  SELECT 1 FROM transactions local_txn 
			  WHERE local_txn.transaction_id = pg_txn.transaction_id
		  )
		ORDER BY pg_txn.created_at DESC, pg_txn.transaction_id DESC
		LIMIT 1`

	whereClause := ws.buildTimestampWhereClause(watermark)
	return fmt.Sprintf(baseQuery, whereClause)
}

func (ws *WatermarkSyncer) buildTimestampWhereClause(watermark *SyncWatermark) string {
	timestampStr := watermark.LastSyncTimestamp.Format("2006-01-02 15:04:05")

	if watermark.LastTransactionID != "" {
		return fmt.Sprintf(
			"AND (pg_txn.created_at > '%s' OR (pg_txn.created_at = '%s' AND pg_txn.transaction_id != '%s'))",
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

func (ws *WatermarkSyncer) copyIdentities(db *sql.DB, watermark *SyncWatermark) (int64, string, time.Time, error) {
	query := ws.buildIdentitiesCopyQuery(watermark)

	result, err := db.Exec(query)
	if err != nil {
		return 0, "", watermark.LastIdentitySyncTimestamp, fmt.Errorf("failed to copy identities: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Warn().Err(err).Msg("Could not determine number of identities copied")
		rowsAffected = 0
	}

	var lastIdentityID string
	var newWatermark time.Time

	if rowsAffected > 0 {
		maxTimestampQuery := ws.buildIdentitiesMaxTimestampQuery(watermark)
		var maxTimestamp sql.NullTime
		err = db.QueryRow(maxTimestampQuery).Scan(&maxTimestamp)
		if err != nil && err != sql.ErrNoRows {
			log.Warn().Err(err).Msg("Could not determine max identity timestamp")
		}

		if maxTimestamp.Valid {
			newWatermark = maxTimestamp.Time
		} else {
			newWatermark = time.Now()
		}

		lastIDQuery := ws.buildLastIdentityIDQuery(watermark)
		var lastID sql.NullString
		err = db.QueryRow(lastIDQuery).Scan(&lastID)
		if err != nil && err != sql.ErrNoRows {
			log.Warn().Err(err).Msg("Could not determine last identity ID")
		}

		if lastID.Valid {
			lastIdentityID = lastID.String
		} else {
			lastIdentityID = watermark.LastIdentityID
		}
	} else {
		newWatermark = watermark.LastIdentitySyncTimestamp
		lastIdentityID = watermark.LastIdentityID
	}

	return rowsAffected, lastIdentityID, newWatermark, nil
}

func (ws *WatermarkSyncer) buildIdentitiesCopyQuery(watermark *SyncWatermark) string {
	timestampStr := watermark.LastIdentitySyncTimestamp.Format("2006-01-02 15:04:05")

	whereClause := fmt.Sprintf("AND pg_id.created_at > '%s'", timestampStr)
	if watermark.LastIdentityID != "" {
		whereClause = fmt.Sprintf(
			"AND (pg_id.created_at > '%s' OR (pg_id.created_at = '%s' AND pg_id.identity_id != '%s'))",
			timestampStr, timestampStr, watermark.LastIdentityID,
		)
	}

	return fmt.Sprintf(`
		INSERT OR REPLACE INTO identity (
			identity_id, first_name, last_name, organization_name, 
			email_address, identity_type, country, dob, created_at, metadata
		)
		SELECT 
			pg_id.identity_id,
			COALESCE(pg_id.first_name, '') as first_name,
			COALESCE(pg_id.last_name, '') as last_name,
			COALESCE(pg_id.organization_name, '') as organization_name,
			COALESCE(pg_id.email_address, '') as email_address,
			COALESCE(pg_id.identity_type, '') as identity_type,
			COALESCE(pg_id.country, '') as country,
			COALESCE(pg_id.dob, '') as dob,
			COALESCE(pg_id.created_at, NOW()) as created_at,
			COALESCE(pg_id.meta_data, '{}') as metadata
		FROM postgres_db.blnk.identity pg_id
		WHERE 1=1
		  %s
		  AND NOT EXISTS (
			  SELECT 1 FROM identity local_id 
			  WHERE local_id.identity_id = pg_id.identity_id
		  )
		ORDER BY pg_id.created_at ASC
		LIMIT %d
	`, whereClause, ws.config.BatchSize)
}

func (ws *WatermarkSyncer) buildIdentitiesMaxTimestampQuery(watermark *SyncWatermark) string {
	timestampStr := watermark.LastIdentitySyncTimestamp.Format("2006-01-02 15:04:05")

	whereClause := fmt.Sprintf("AND pg_id.created_at > '%s'", timestampStr)
	if watermark.LastIdentityID != "" {
		whereClause = fmt.Sprintf(
			"AND (pg_id.created_at > '%s' OR (pg_id.created_at = '%s' AND pg_id.identity_id != '%s'))",
			timestampStr, timestampStr, watermark.LastIdentityID,
		)
	}

	return fmt.Sprintf(`
		SELECT MAX(created_at) 
		FROM (
			SELECT pg_id.created_at
			FROM postgres_db.blnk.identity pg_id
			WHERE 1=1
			  %s
			  AND NOT EXISTS (
				  SELECT 1 FROM identity local_id 
				  WHERE local_id.identity_id = pg_id.identity_id
			  )
			ORDER BY pg_id.created_at ASC
			LIMIT %d
		) AS batch_identities
	`, whereClause, ws.config.BatchSize)
}

func (ws *WatermarkSyncer) buildLastIdentityIDQuery(watermark *SyncWatermark) string {
	timestampStr := watermark.LastIdentitySyncTimestamp.Format("2006-01-02 15:04:05")

	whereClause := fmt.Sprintf("AND pg_id.created_at > '%s'", timestampStr)
	if watermark.LastIdentityID != "" {
		whereClause = fmt.Sprintf(
			"AND (pg_id.created_at > '%s' OR (pg_id.created_at = '%s' AND pg_id.identity_id != '%s'))",
			timestampStr, timestampStr, watermark.LastIdentityID,
		)
	}

	return fmt.Sprintf(`
		SELECT pg_id.identity_id
		FROM postgres_db.blnk.identity pg_id
		WHERE 1=1
		  %s
		  AND NOT EXISTS (
			  SELECT 1 FROM identity local_id 
			  WHERE local_id.identity_id = pg_id.identity_id
		  )
		ORDER BY pg_id.created_at DESC, pg_id.identity_id DESC
		LIMIT 1
	`, whereClause)
}

func (ws *WatermarkSyncer) copyBalances(db *sql.DB, watermark *SyncWatermark) (int64, string, time.Time, error) {
	query := ws.buildBalancesCopyQuery(watermark)

	result, err := db.Exec(query)
	if err != nil {
		return 0, "", watermark.LastBalanceSyncTimestamp, fmt.Errorf("failed to copy balances: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Warn().Err(err).Msg("Could not determine number of balances copied")
		rowsAffected = 0
	}

	var lastBalanceID string
	var newWatermark time.Time

	if rowsAffected > 0 {
		maxTimestampQuery := ws.buildBalancesMaxTimestampQuery(watermark)
		var maxTimestamp sql.NullTime
		err = db.QueryRow(maxTimestampQuery).Scan(&maxTimestamp)
		if err != nil && err != sql.ErrNoRows {
			log.Warn().Err(err).Msg("Could not determine max balance timestamp")
		}

		if maxTimestamp.Valid {
			newWatermark = maxTimestamp.Time
		} else {
			newWatermark = time.Now()
		}

		lastIDQuery := ws.buildLastBalanceIDQuery(watermark)
		var lastID sql.NullString
		err = db.QueryRow(lastIDQuery).Scan(&lastID)
		if err != nil && err != sql.ErrNoRows {
			log.Warn().Err(err).Msg("Could not determine last balance ID")
		}

		if lastID.Valid {
			lastBalanceID = lastID.String
		} else {
			lastBalanceID = watermark.LastBalanceID
		}
	} else {
		newWatermark = watermark.LastBalanceSyncTimestamp
		lastBalanceID = watermark.LastBalanceID
	}

	return rowsAffected, lastBalanceID, newWatermark, nil
}

func (ws *WatermarkSyncer) buildBalancesCopyQuery(watermark *SyncWatermark) string {
	timestampStr := watermark.LastBalanceSyncTimestamp.Format("2006-01-02 15:04:05")

	whereClause := fmt.Sprintf("AND pg_bal.created_at > '%s'", timestampStr)
	if watermark.LastBalanceID != "" {
		whereClause = fmt.Sprintf(
			"AND (pg_bal.created_at > '%s' OR (pg_bal.created_at = '%s' AND pg_bal.balance_id != '%s'))",
			timestampStr, timestampStr, watermark.LastBalanceID,
		)
	}

	return fmt.Sprintf(`
		INSERT OR REPLACE INTO balances (
			balance_id, ledger_id, identity_id, currency, 
			balance, inflight_balance, credit_balance, debit_balance,
			inflight_credit_balance, inflight_debit_balance,
			indicator, created_at, metadata
		)
		SELECT 
			pg_bal.balance_id,
			COALESCE(pg_bal.ledger_id, '') as ledger_id,
			COALESCE(pg_bal.identity_id, '') as identity_id,
			COALESCE(pg_bal.currency, '') as currency,
			COALESCE(pg_bal.balance, 0.0) as balance,
			COALESCE(pg_bal.inflight_balance, 0.0) as inflight_balance,
			COALESCE(pg_bal.credit_balance, 0.0) as credit_balance,
			COALESCE(pg_bal.debit_balance, 0.0) as debit_balance,
			COALESCE(pg_bal.inflight_credit_balance, 0.0) as inflight_credit_balance,
			COALESCE(pg_bal.inflight_debit_balance, 0.0) as inflight_debit_balance,
			COALESCE(pg_bal.indicator, '') as indicator,
			COALESCE(pg_bal.created_at, NOW()) as created_at,
			COALESCE(pg_bal.meta_data, '{}') as metadata
		FROM postgres_db.blnk.balances pg_bal
		WHERE 1=1
		  %s
		  AND NOT EXISTS (
			  SELECT 1 FROM balances local_bal 
			  WHERE local_bal.balance_id = pg_bal.balance_id
		  )
		ORDER BY pg_bal.created_at ASC
		LIMIT %d
	`, whereClause, ws.config.BatchSize)
}

func (ws *WatermarkSyncer) buildBalancesMaxTimestampQuery(watermark *SyncWatermark) string {
	timestampStr := watermark.LastBalanceSyncTimestamp.Format("2006-01-02 15:04:05")

	whereClause := fmt.Sprintf("AND pg_bal.created_at > '%s'", timestampStr)
	if watermark.LastBalanceID != "" {
		whereClause = fmt.Sprintf(
			"AND (pg_bal.created_at > '%s' OR (pg_bal.created_at = '%s' AND pg_bal.balance_id != '%s'))",
			timestampStr, timestampStr, watermark.LastBalanceID,
		)
	}

	return fmt.Sprintf(`
		SELECT MAX(created_at) 
		FROM (
			SELECT pg_bal.created_at
			FROM postgres_db.blnk.balances pg_bal
			WHERE 1=1
			  %s
			  AND NOT EXISTS (
				  SELECT 1 FROM balances local_bal 
				  WHERE local_bal.balance_id = pg_bal.balance_id
			  )
			ORDER BY pg_bal.created_at ASC
			LIMIT %d
		) AS batch_balances
	`, whereClause, ws.config.BatchSize)
}

func (ws *WatermarkSyncer) buildLastBalanceIDQuery(watermark *SyncWatermark) string {
	timestampStr := watermark.LastBalanceSyncTimestamp.Format("2006-01-02 15:04:05")

	whereClause := fmt.Sprintf("AND pg_bal.created_at > '%s'", timestampStr)
	if watermark.LastBalanceID != "" {
		whereClause = fmt.Sprintf(
			"AND (pg_bal.created_at > '%s' OR (pg_bal.created_at = '%s' AND pg_bal.balance_id != '%s'))",
			timestampStr, timestampStr, watermark.LastBalanceID,
		)
	}

	return fmt.Sprintf(`
		SELECT pg_bal.balance_id
		FROM postgres_db.blnk.balances pg_bal
		WHERE 1=1
		  %s
		  AND NOT EXISTS (
			  SELECT 1 FROM balances local_bal 
			  WHERE local_bal.balance_id = pg_bal.balance_id
		  )
		ORDER BY pg_bal.created_at DESC, pg_bal.balance_id DESC
		LIMIT 1
	`, whereClause)
}

func (ws *WatermarkSyncer) copyLedgers(db *sql.DB, watermark *SyncWatermark) (int64, string, time.Time, error) {
	query := ws.buildLedgersCopyQuery(watermark)

	result, err := db.Exec(query)
	if err != nil {
		return 0, "", watermark.LastLedgerSyncTimestamp, fmt.Errorf("failed to copy ledgers: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Warn().Err(err).Msg("Could not determine number of ledgers copied")
		rowsAffected = 0
	}

	var lastLedgerID string
	var newWatermark time.Time

	if rowsAffected > 0 {
		maxTimestampQuery := ws.buildLedgersMaxTimestampQuery(watermark)
		var maxTimestamp sql.NullTime
		err = db.QueryRow(maxTimestampQuery).Scan(&maxTimestamp)
		if err != nil && err != sql.ErrNoRows {
			log.Warn().Err(err).Msg("Could not determine max ledger timestamp")
		}

		if maxTimestamp.Valid {
			newWatermark = maxTimestamp.Time
		} else {
			newWatermark = time.Now()
		}

		lastIDQuery := ws.buildLastLedgerIDQuery(watermark)
		var lastID sql.NullString
		err = db.QueryRow(lastIDQuery).Scan(&lastID)
		if err != nil && err != sql.ErrNoRows {
			log.Warn().Err(err).Msg("Could not determine last ledger ID")
		}

		if lastID.Valid {
			lastLedgerID = lastID.String
		} else {
			lastLedgerID = watermark.LastLedgerID
		}
	} else {
		newWatermark = watermark.LastLedgerSyncTimestamp
		lastLedgerID = watermark.LastLedgerID
	}

	return rowsAffected, lastLedgerID, newWatermark, nil
}

func (ws *WatermarkSyncer) buildLedgersCopyQuery(watermark *SyncWatermark) string {
	timestampStr := watermark.LastLedgerSyncTimestamp.Format("2006-01-02 15:04:05")

	whereClause := fmt.Sprintf("AND pg_led.created_at > '%s'", timestampStr)
	if watermark.LastLedgerID != "" {
		whereClause = fmt.Sprintf(
			"AND (pg_led.created_at > '%s' OR (pg_led.created_at = '%s' AND pg_led.ledger_id != '%s'))",
			timestampStr, timestampStr, watermark.LastLedgerID,
		)
	}

	return fmt.Sprintf(`
		INSERT OR REPLACE INTO ledgers (
			ledger_id, name, created_at, metadata
		)
		SELECT 
			pg_led.ledger_id,
			COALESCE(pg_led.name, '') as name,
			COALESCE(pg_led.created_at, NOW()) as created_at,
			COALESCE(pg_led.meta_data, '{}') as metadata
		FROM postgres_db.blnk.ledgers pg_led
		WHERE 1=1
		  %s
		  AND NOT EXISTS (
			  SELECT 1 FROM ledgers local_led 
			  WHERE local_led.ledger_id = pg_led.ledger_id
		  )
		ORDER BY pg_led.created_at ASC
		LIMIT %d
	`, whereClause, ws.config.BatchSize)
}

func (ws *WatermarkSyncer) buildLedgersMaxTimestampQuery(watermark *SyncWatermark) string {
	timestampStr := watermark.LastLedgerSyncTimestamp.Format("2006-01-02 15:04:05")

	whereClause := fmt.Sprintf("AND pg_led.created_at > '%s'", timestampStr)
	if watermark.LastLedgerID != "" {
		whereClause = fmt.Sprintf(
			"AND (pg_led.created_at > '%s' OR (pg_led.created_at = '%s' AND pg_led.ledger_id != '%s'))",
			timestampStr, timestampStr, watermark.LastLedgerID,
		)
	}

	return fmt.Sprintf(`
		SELECT MAX(created_at) 
		FROM (
			SELECT pg_led.created_at
			FROM postgres_db.blnk.ledgers pg_led
			WHERE 1=1
			  %s
			  AND NOT EXISTS (
				  SELECT 1 FROM ledgers local_led 
				  WHERE local_led.ledger_id = pg_led.ledger_id
			  )
			ORDER BY pg_led.created_at ASC
			LIMIT %d
		) AS batch_ledgers
	`, whereClause, ws.config.BatchSize)
}

func (ws *WatermarkSyncer) buildLastLedgerIDQuery(watermark *SyncWatermark) string {
	timestampStr := watermark.LastLedgerSyncTimestamp.Format("2006-01-02 15:04:05")

	whereClause := fmt.Sprintf("AND pg_led.created_at > '%s'", timestampStr)
	if watermark.LastLedgerID != "" {
		whereClause = fmt.Sprintf(
			"AND (pg_led.created_at > '%s' OR (pg_led.created_at = '%s' AND pg_led.ledger_id != '%s'))",
			timestampStr, timestampStr, watermark.LastLedgerID,
		)
	}

	return fmt.Sprintf(`
		SELECT pg_led.ledger_id
		FROM postgres_db.blnk.ledgers pg_led
		WHERE 1=1
		  %s
		  AND NOT EXISTS (
			  SELECT 1 FROM ledgers local_led 
			  WHERE local_led.ledger_id = pg_led.ledger_id
		  )
		ORDER BY pg_led.created_at DESC, pg_led.ledger_id DESC
		LIMIT 1
	`, whereClause)
}

func (ws *WatermarkSyncer) getWatermark(db *sql.DB) (*SyncWatermark, error) {
	transactionDefault := ws.config.TransactionStartTime.Format("2006-01-02 15:04:05")
	identityDefault := ws.config.IdentityStartTime.Format("2006-01-02 15:04:05")
	balanceDefault := ws.config.BalanceStartTime.Format("2006-01-02 15:04:05")
	ledgerDefault := ws.config.LedgerStartTime.Format("2006-01-02 15:04:05")

	query := fmt.Sprintf(`
		SELECT 
			id, 
			-- Transaction fields
			last_sync_timestamp, 
			COALESCE(last_transaction_id, ''), 
			total_synced_count,
			-- Identity fields
			COALESCE(last_identity_sync_timestamp, '%s'),
			COALESCE(last_identity_id, ''),
			COALESCE(total_identities_synced, 0),
			-- Balance fields
			COALESCE(last_balance_sync_timestamp, '%s'),
			COALESCE(last_balance_id, ''),
			COALESCE(total_balances_synced, 0),
			-- Ledger fields
			COALESCE(last_ledger_sync_timestamp, '%s'),
			COALESCE(last_ledger_id, ''),
			COALESCE(total_ledgers_synced, 0),
			-- General fields
			COALESCE(last_sync_completed_at, '%s'), 
			sync_status, 
			created_at, 
			updated_at
		FROM sync_watermark 
		WHERE id = 1
	`, identityDefault, balanceDefault, ledgerDefault, transactionDefault)

	var w SyncWatermark
	err := db.QueryRow(query).Scan(
		&w.ID,
		// Transaction fields
		&w.LastSyncTimestamp,
		&w.LastTransactionID,
		&w.TotalSyncedCount,
		// Identity fields
		&w.LastIdentitySyncTimestamp,
		&w.LastIdentityID,
		&w.TotalIdentitiesSynced,
		// Balance fields
		&w.LastBalanceSyncTimestamp,
		&w.LastBalanceID,
		&w.TotalBalancesSynced,
		// Ledger fields
		&w.LastLedgerSyncTimestamp,
		&w.LastLedgerID,
		&w.TotalLedgersSynced,
		// General fields
		&w.LastSyncCompletedAt,
		&w.SyncStatus,
		&w.CreatedAt,
		&w.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get watermark: %w", err)
	}

	return &w, nil
}

func (ws *WatermarkSyncer) updateWatermarkFull(db *sql.DB, result *SyncResult) error {
	transactionWatermark := result.TransactionWatermark
	if transactionWatermark.IsZero() || transactionWatermark.Before(ws.config.TransactionStartTime) {
		transactionWatermark = ws.config.TransactionStartTime
	}

	identityWatermark := result.IdentityWatermark
	if identityWatermark.IsZero() || identityWatermark.Before(ws.config.IdentityStartTime) {
		identityWatermark = ws.config.IdentityStartTime
	}

	balanceWatermark := result.BalanceWatermark
	if balanceWatermark.IsZero() || balanceWatermark.Before(ws.config.BalanceStartTime) {
		balanceWatermark = ws.config.BalanceStartTime
	}

	ledgerWatermark := result.LedgerWatermark
	if ledgerWatermark.IsZero() || ledgerWatermark.Before(ws.config.LedgerStartTime) {
		ledgerWatermark = ws.config.LedgerStartTime
	}

	query := `
		UPDATE sync_watermark 
		SET 
			-- Transaction fields
			last_sync_timestamp = ?,
			last_transaction_id = ?,
			total_synced_count = total_synced_count + ?,
			-- Identity fields
			last_identity_sync_timestamp = ?,
			last_identity_id = ?,
			total_identities_synced = total_identities_synced + ?,
		-- Balance fields
		last_balance_sync_timestamp = ?,
		last_balance_id = ?,
		total_balances_synced = total_balances_synced + ?,
		-- Ledger fields
		last_ledger_sync_timestamp = ?,
		last_ledger_id = ?,
		total_ledgers_synced = total_ledgers_synced + ?,
		-- General fields
			last_sync_completed_at = CURRENT_TIMESTAMP,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = 1
	`

	_, err := db.Exec(query,
		transactionWatermark.Format("2006-01-02 15:04:05"),
		result.LastTransactionID,
		result.TransactionsSynced,
		identityWatermark.Format("2006-01-02 15:04:05"),
		result.LastIdentityID,
		result.IdentitiesSynced,
		balanceWatermark.Format("2006-01-02 15:04:05"),
		result.LastBalanceID,
		result.BalancesSynced,
		ledgerWatermark.Format("2006-01-02 15:04:05"),
		result.LastLedgerID,
		result.LedgersSynced,
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
	db, err := GetDB()
	if err != nil {
		return nil, fmt.Errorf("failed to get DuckDB connection: %w", err)
	}

	watermark, err := ws.getWatermark(db)
	if err != nil {
		return nil, err
	}

	log.Debug().
		Int64("transactions_synced", watermark.TotalSyncedCount).
		Int64("identities_synced", watermark.TotalIdentitiesSynced).
		Int64("balances_synced", watermark.TotalBalancesSynced).
		Int64("ledgers_synced", watermark.TotalLedgersSynced).
		Time("last_transaction_sync", watermark.LastSyncTimestamp).
		Time("last_identity_sync", watermark.LastIdentitySyncTimestamp).
		Time("last_balance_sync", watermark.LastBalanceSyncTimestamp).
		Time("last_ledger_sync", watermark.LastLedgerSyncTimestamp).
		Str("status", watermark.SyncStatus).
		Msg("Current sync status")

	return watermark, nil
}

func (ws *WatermarkSyncer) ForceSync() error {
	log.Info().Msg("Force sync triggered")
	return ws.performSync()
}

func (ws *WatermarkSyncer) ResetWatermark() error {
	db, err := GetDB()
	if err != nil {
		return fmt.Errorf("failed to get DuckDB connection: %w", err)
	}

	transactionStart := ws.config.TransactionStartTime.Format("2006-01-02 15:04:05")
	identityStart := ws.config.IdentityStartTime.Format("2006-01-02 15:04:05")
	balanceStart := ws.config.BalanceStartTime.Format("2006-01-02 15:04:05")
	ledgerStart := ws.config.LedgerStartTime.Format("2006-01-02 15:04:05")

	query := fmt.Sprintf(`
		UPDATE sync_watermark 
		SET 
			last_sync_timestamp = '%s',
			last_transaction_id = NULL,
			total_synced_count = 0,
			last_identity_sync_timestamp = '%s',
			last_identity_id = NULL,
			total_identities_synced = 0,
			last_balance_sync_timestamp = '%s',
			last_balance_id = NULL,
			total_balances_synced = 0,
			last_ledger_sync_timestamp = '%s',
			last_ledger_id = NULL,
			total_ledgers_synced = 0,
			sync_status = 'idle',
			updated_at = CURRENT_TIMESTAMP
		WHERE id = 1
	`, transactionStart, identityStart, balanceStart, ledgerStart)

	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to reset watermark: %w", err)
	}

	log.Info().Msg("Watermark reset successfully")
	return nil
}
