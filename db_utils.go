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
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/rs/zerolog/log"
)

const instructionDBDir = "blnk_agent"

const instructionDBFilename = "instructions.db"

var instructionDB *sql.DB

const dbDir = "blnk_agent"

const dbFilename = "blnk.db"

var (
	transactionsDB *sql.DB
	dbMutex        sync.RWMutex
)

var ErrTransactionNotFound = errors.New("transaction not found")

type Instruction struct {
	ID          int64          `json:"id"`
	Name        string         `json:"name"`
	Text        string         `json:"text"`
	Description string         `json:"description"`
	DSLJSON     sql.NullString `json:"dsl_json,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

func getInstructionDBPath() (string, error) {
	if err := os.MkdirAll(instructionDBDir, 0755); err != nil {
		log.Error().Err(err).Str("dir", instructionDBDir).Msg("Error creating instruction data directory")
		return "", err
	}
	return filepath.Join(instructionDBDir, instructionDBFilename), nil
}

func ensureInstructionSchema(db *sql.DB) error {
	const sequenceName = "instructions_id_seq"

	seqQuery := fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS %s START 1;`, sequenceName)
	_, err := db.Exec(seqQuery)
	if err != nil {
		log.Error().Err(err).Str("sequenceName", sequenceName).Msg("Failed to create sequence")
		return fmt.Errorf("failed to create sequence %s: %w", sequenceName, err)
	}

	tableQuery := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS instructions (
		id BIGINT PRIMARY KEY DEFAULT nextval('%s'),
		name TEXT NOT NULL UNIQUE,
		text TEXT NOT NULL,
		description TEXT,
		dsl_json JSON NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`, sequenceName)
	_, err = db.Exec(tableQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create instructions table with explicit sequence")
		return fmt.Errorf("failed to create instructions table: %w", err)
	}

	return nil
}

func InitInstructionDB() error {
	dbPath, err := getInstructionDBPath()
	if err != nil {
		return fmt.Errorf("failed to get instruction database path: %w", err)
	}
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		log.Error().Err(err).Str("db_path", dbPath).Msg("Error opening database for instructions")
		return err
	}

	if err = db.Ping(); err != nil {
		log.Error().Err(err).Str("db_path", dbPath).Msg("Failed to ping instruction database")
		db.Close()
		return err
	}

	if err := ensureInstructionSchema(db); err != nil {
		db.Close()
		return err
	}
	instructionDB = db
	return nil
}

func CloseInstructionDB() {
	if instructionDB != nil {
		err := instructionDB.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing instruction database")
		} else {
			log.Info().Msg("Instruction database connection closed.")
		}
	}
}

func createInstructionRecord(db *sql.DB, name, text, description string) (int64, error) {
	query := `
		INSERT INTO instructions (name, text, description)
		VALUES (?, ?, ?)
		RETURNING id;
	`
	var id int64
	err := db.QueryRow(query, name, text, description).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func getDBPath() (string, error) {
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		log.Error().Err(err).Str("dir", dbDir).Msg("Error creating data directory")
		return "", err
	}
	return filepath.Join(dbDir, dbFilename), nil
}

func ensureSchema(db *sql.DB) error {
	tableQuery := `
	CREATE TABLE IF NOT EXISTS transactions (
		transaction_id VARCHAR NOT NULL,
		amount DOUBLE,
		currency VARCHAR,
		source VARCHAR,
		destination VARCHAR,
		timestamp TIMESTAMP,
		description VARCHAR,
		metadata JSON,
		PRIMARY KEY (transaction_id)
	);
	`
	_, err := db.Exec(tableQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create transactions table")
		return fmt.Errorf("failed to create transactions table: %w", err)
	}

	identityQuery := `
	CREATE TABLE IF NOT EXISTS identity (
		identity_id VARCHAR NOT NULL,
		first_name VARCHAR,
		last_name VARCHAR,
		organization_name VARCHAR,
		email_address VARCHAR,
		identity_type VARCHAR,
		country VARCHAR,
		dob VARCHAR,
		created_at TIMESTAMP,
		metadata JSON,
		PRIMARY KEY (identity_id)
	);
	`
	_, err = db.Exec(identityQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create identity table")
		return fmt.Errorf("failed to create identity table: %w", err)
	}

	ledgersQuery := `
	CREATE TABLE IF NOT EXISTS ledgers (
		ledger_id VARCHAR NOT NULL,
		name VARCHAR,
		created_at TIMESTAMP,
		metadata JSON,
		PRIMARY KEY (ledger_id)
	);
	`
	_, err = db.Exec(ledgersQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create ledgers table")
		return fmt.Errorf("failed to create ledgers table: %w", err)
	}

	balancesQuery := `
	CREATE TABLE IF NOT EXISTS balances (
		balance_id VARCHAR NOT NULL,
		ledger_id VARCHAR,
		identity_id VARCHAR,
		currency VARCHAR,
		balance DOUBLE,
		inflight_balance DOUBLE,
		credit_balance DOUBLE,
		debit_balance DOUBLE,
		inflight_credit_balance DOUBLE,
		inflight_debit_balance DOUBLE,
		indicator VARCHAR,
		created_at TIMESTAMP,
		metadata JSON,
		PRIMARY KEY (balance_id)
	);
	`
	_, err = db.Exec(balancesQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create balances table")
		return fmt.Errorf("failed to create balances table: %w", err)
	}

	watermarkQuery := `
	CREATE TABLE IF NOT EXISTS sync_watermark (
		id INTEGER PRIMARY KEY DEFAULT 1,
		-- Transactions watermark
		last_sync_timestamp TIMESTAMP,
		last_transaction_id VARCHAR,
		total_synced_count BIGINT DEFAULT 0,
		-- Identities watermark
		last_identity_sync_timestamp TIMESTAMP,
		last_identity_id VARCHAR,
		total_identities_synced BIGINT DEFAULT 0,
		-- Balances watermark
		last_balance_sync_timestamp TIMESTAMP,
		last_balance_id VARCHAR,
		total_balances_synced BIGINT DEFAULT 0,
		-- Ledgers watermark
		last_ledger_sync_timestamp TIMESTAMP,
		last_ledger_id VARCHAR,
		total_ledgers_synced BIGINT DEFAULT 0,
		-- General sync info
		last_sync_completed_at TIMESTAMP,
		sync_status VARCHAR DEFAULT 'idle',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		CHECK (id = 1)
	);
	`
	_, err = db.Exec(watermarkQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create sync_watermark table")
		return fmt.Errorf("failed to create sync_watermark table: %w", err)
	}

	initWatermarkQuery := `
	INSERT OR IGNORE INTO sync_watermark (
		id, 
		last_sync_timestamp, 
		last_identity_sync_timestamp,
		last_balance_sync_timestamp,
		last_ledger_sync_timestamp,
		sync_status
	) 
	VALUES (1, '1970-01-01 00:00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:00', 'idle');
	`
	_, err = db.Exec(initWatermarkQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to initialize sync watermark")
		return fmt.Errorf("failed to initialize sync watermark: %w", err)
	}

	sourceIndexQuery := `CREATE INDEX IF NOT EXISTS idx_transactions_source ON transactions (source);`
	_, err = db.Exec(sourceIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on source")
		return fmt.Errorf("failed to create index on source: %w", err)
	}

	destIndexQuery := `CREATE INDEX IF NOT EXISTS idx_transactions_destination ON transactions (destination);`
	_, err = db.Exec(destIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on destination")
		return fmt.Errorf("failed to create index on destination: %w", err)
	}

	timestampIndexQuery := `CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions (timestamp);`
	_, err = db.Exec(timestampIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on timestamp")
		return fmt.Errorf("failed to create index on timestamp: %w", err)
	}

	identityCreatedAtIndexQuery := `CREATE INDEX IF NOT EXISTS idx_identity_created_at ON identity (created_at);`
	_, err = db.Exec(identityCreatedAtIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on identity created_at")
		return fmt.Errorf("failed to create index on identity created_at: %w", err)
	}

	identityTypeIndexQuery := `CREATE INDEX IF NOT EXISTS idx_identity_type ON identity (identity_type);`
	_, err = db.Exec(identityTypeIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on identity type")
		return fmt.Errorf("failed to create index on identity type: %w", err)
	}

	ledgerCreatedAtIndexQuery := `CREATE INDEX IF NOT EXISTS idx_ledgers_created_at ON ledgers (created_at);`
	_, err = db.Exec(ledgerCreatedAtIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on ledgers created_at")
		return fmt.Errorf("failed to create index on ledgers created_at: %w", err)
	}

	ledgerNameIndexQuery := `CREATE INDEX IF NOT EXISTS idx_ledgers_name ON ledgers (name);`
	_, err = db.Exec(ledgerNameIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on ledgers name")
		return fmt.Errorf("failed to create index on ledgers name: %w", err)
	}

	balanceIdentityIndexQuery := `CREATE INDEX IF NOT EXISTS idx_balances_identity_id ON balances (identity_id);`
	_, err = db.Exec(balanceIdentityIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on balances identity_id")
		return fmt.Errorf("failed to create index on balances identity_id: %w", err)
	}

	balanceLedgerIndexQuery := `CREATE INDEX IF NOT EXISTS idx_balances_ledger_id ON balances (ledger_id);`
	_, err = db.Exec(balanceLedgerIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on balances ledger_id")
		return fmt.Errorf("failed to create index on balances ledger_id: %w", err)
	}

	balanceCreatedAtIndexQuery := `CREATE INDEX IF NOT EXISTS idx_balances_created_at ON balances (created_at);`
	_, err = db.Exec(balanceCreatedAtIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on balances created_at")
		return fmt.Errorf("failed to create index on balances created_at: %w", err)
	}

	balanceCurrencyIndexQuery := `CREATE INDEX IF NOT EXISTS idx_balances_currency ON balances (currency);`
	_, err = db.Exec(balanceCurrencyIndexQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create index on balances currency")
		return fmt.Errorf("failed to create index on balances currency: %w", err)
	}

	return nil
}

func InitTransactionsDB() error {
	dbPath, err := getDBPath()
	if err != nil {
		return fmt.Errorf("failed to get transactions database path: %w", err)
	}

	connStr := fmt.Sprintf("%s?access_mode=READ_WRITE&threads=1&memory_limit=2GiB&checkpoint_threshold=64MiB", dbPath)
	db, err := sql.Open("duckdb", connStr)
	if err != nil {
		log.Error().Err(err).Str("db_path", dbPath).Msg("Error opening database for transactions")
		return err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(10 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Error().Err(err).Str("db_path", dbPath).Msg("Failed to ping transactions database")
		db.Close()
		return err
	}

	pragmas := []string{
		"SET memory_limit='2GiB'",
		"SET temp_directory='blnk_agent/duckdb_temp'",
		"SET threads=1",
		"SET checkpoint_threshold='64MiB'",
		"SET enable_progress_bar=false",
		"SET preserve_insertion_order=false",
		"SET null_order='nulls_first'",
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			log.Error().Err(err).Str("pragma", pragma).Msg("Failed to set DuckDB pragma")
			db.Close()
			return fmt.Errorf("failed to set pragma %s: %w", pragma, err)
		}
	}

	if err := os.MkdirAll("blnk_agent/duckdb_temp", 0755); err != nil {
		log.Error().Err(err).Msg("Failed to create DuckDB temp directory")
		db.Close()
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	if err := ensureSchema(db); err != nil {
		db.Close()
		return err
	}

	dbMutex.Lock()
	transactionsDB = db
	dbMutex.Unlock()

	go periodicCheckpoint()
	return nil
}

func CloseTransactionsDB() {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if transactionsDB != nil {
		if _, err := transactionsDB.Exec("CHECKPOINT"); err != nil {
			log.Error().Err(err).Msg("Failed to checkpoint database before closing")
		}

		err := transactionsDB.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing transactions database")
		} else {
			log.Info().Msg("Transactions database connection closed.")
		}
		transactionsDB = nil
	}
}

func getDB() (*sql.DB, error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	if transactionsDB == nil {
		return nil, fmt.Errorf("transactions database not initialized")
	}
	return transactionsDB, nil
}

func GetDB() (*sql.DB, error) {
	return getDB()
}
func updateTransactionMetadataInDB(txID string, metadata map[string]interface{}) error {

	db, err := getDB()
	if err != nil {
		log.Error().Str("tx_id", txID).Msg("Transactions database not initialized")
		return err
	}

	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = updateTransactionMetadataAttempt(db, txID, metadata)
		if err == nil {
			return nil
		}

		if isRecoverableError(err) && attempt < maxRetries {
			log.Warn().
				Err(err).
				Str("tx_id", txID).
				Int("attempt", attempt).
				Int("max_retries", maxRetries).
				Msg("Retrying metadata update after recoverable error")

			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
			continue
		}

		return err
	}

	return err
}

func isRecoverableError(err error) bool {
	errStr := err.Error()
	return contains(errStr, "Could not read enough bytes") ||
		contains(errStr, "IO Error") ||
		contains(errStr, "database is locked") ||
		contains(errStr, "connection reset")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			containsHelper(s, substr)))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func updateTransactionMetadataAttempt(db *sql.DB, txID string, metadata map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	})
	if err != nil {
		log.Error().Err(err).Str("tx_id", txID).Msg("Failed to begin transaction")
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Error().Err(rollbackErr).Str("tx_id", txID).Msg("Failed to rollback transaction")
			}
		}
	}()

	var exists bool
	existsQuery := "SELECT EXISTS(SELECT 1 FROM transactions WHERE transaction_id = ?) as exists"
	err = tx.QueryRowContext(ctx, existsQuery, txID).Scan(&exists)
	if err != nil {
		log.Error().
			Err(err).
			Str("tx_id", txID).
			Str("query", existsQuery).
			Msg("Failed to check if transaction exists")
		return fmt.Errorf("failed to check if transaction exists: %w", err)
	}

	if !exists {
		return ErrTransactionNotFound
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		log.Error().Err(err).Str("tx_id", txID).Msg("Failed to marshal metadata")
		return fmt.Errorf("failed to serialize metadata for update: %w", err)
	}

	metadataStr := string(metadataJSON)

	var testParse map[string]interface{}
	if err := json.Unmarshal([]byte(metadataStr), &testParse); err != nil {
		log.Error().Err(err).Str("tx_id", txID).Msg("Generated invalid JSON")
		return fmt.Errorf("generated invalid JSON: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, `UPDATE transactions SET metadata = ? WHERE transaction_id = ?`)
	if err != nil {
		log.Error().Err(err).Str("tx_id", txID).Msg("Failed to prepare update statement")
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer stmt.Close()

	result, err := stmt.ExecContext(ctx, metadataStr, txID)
	if err != nil {
		log.Error().Err(err).Str("tx_id", txID).Msg("Failed to execute update statement")
		return fmt.Errorf("failed to execute update statement: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Error().Err(err).Str("tx_id", txID).Msg("Failed to get rows affected")
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		log.Error().Str("tx_id", txID).Msg("No rows were updated")
		return fmt.Errorf("no rows were updated for transaction %s", txID)
	}

	if err = tx.Commit(); err != nil {
		log.Error().Err(err).Str("tx_id", txID).Msg("Failed to commit transaction")
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func periodicCheckpoint() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		if db, err := getDB(); err == nil {
			if _, err := db.Exec("CHECKPOINT"); err != nil {
				log.Error().Err(err).Msg("Failed to perform periodic checkpoint")
			} else {
				log.Debug().Msg("Periodic checkpoint completed successfully")
			}
		}
	}
}
