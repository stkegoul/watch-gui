# Watermark Sync Documentation

## Overview

The **Watermark Sync** system is an incremental data synchronization mechanism that copies data from a PostgreSQL database (Blnk's main database) to a local DuckDB database. It uses a "watermark" pattern to track the last synchronized position, ensuring efficient incremental updates without re-syncing existing data.

The sync system handles **Transactions** (payment and transaction records).

## Table of Contents

1. [How It Works](#how-it-works)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [API Reference](#api-reference)
6. [Examples](#examples)
7. [Troubleshooting](#troubleshooting)
8. [Best Practices](#best-practices)

---

## How It Works

### Watermark Pattern

The watermark sync uses a **timestamp + ID** combination to track synchronization progress:

1. **Last Sync Timestamp**: The `created_at` timestamp of the last synchronized record
2. **Last Record ID**: The ID of the last synchronized record (for tie-breaking when multiple records have the same timestamp)

This dual-key approach ensures:
- **No duplicates**: Records are only synced once
- **No gaps**: All records after the watermark are eventually synced
- **Handles edge cases**: Multiple records with identical timestamps are handled correctly

### Sync Process

Each sync cycle follows these steps:

1. **Load Watermark**: Retrieve the current watermark state from `sync_watermark` table
2. **Attach PostgreSQL**: Connect to the source PostgreSQL database via DuckDB's PostgreSQL extension
3. **Query New Records**: Fetch records where:
   - `created_at > last_sync_timestamp` OR
   - `created_at = last_sync_timestamp AND id != last_record_id`
   - AND the record doesn't already exist in DuckDB
4. **Batch Copy**: Copy records in batches (default: 1000 records per batch)
5. **Update Watermark**: Update the watermark with the new timestamp and ID
6. **Detach PostgreSQL**: Clean up the connection

### Incremental Sync Benefits

- **Efficient**: Only processes new/changed data
- **Resumable**: Can stop and resume without losing progress
- **Low Overhead**: Minimal database load on source system
- **Fault Tolerant**: Retries on failure with configurable attempts

---

## Architecture

### Components

```
┌─────────────────┐         ┌──────────────────┐         ┌─────────────────┐
│   PostgreSQL    │         │  WatermarkSyncer │         │     DuckDB      │
│  (Source DB)    │────────▶│   (Sync Engine) │────────▶│  (Local DB)     │
│                 │         │                  │         │                 │
│ - transactions  │         │ - SyncConfig     │         │ - transactions  │
│                 │         │ - SyncLoop       │         │                 │
│                 │         │ - Watermark      │         │                 │
│                 │         │ - Retry Logic    │         │                 │
└─────────────────┘         └──────────────────┘         └─────────────────┘
                                      │
                                      ▼
                            ┌──────────────────┐
                            │ sync_watermark   │
                            │   (State Table)  │
                            └──────────────────┘
```

### Data Flow

1. **Initialization**: WatermarkSyncer is created with configuration
2. **Start**: Background goroutine starts periodic sync loop
3. **Sync Cycle**:
   - Attach PostgreSQL database
   - Query and copy new records
   - Update watermark
   - Detach PostgreSQL
4. **Monitoring**: Status tracked in `sync_watermark` table

---

## Configuration

### SyncConfig Structure

```go
type SyncConfig struct {
    SyncInterval         time.Duration // How often to run sync (default: 1 second)
    BatchSize            int           // Records per batch (default: 1000)
    MaxRetries           int           // Max retry attempts (default: 3)
    RetryDelay           time.Duration // Delay between retries (default: 30 seconds)
    EnableSync           bool          // Enable/disable sync (default: true)
    TransactionStartTime time.Time     // Starting point for transactions
}
```

### Default Configuration

```go
config := DefaultSyncConfig()
// Returns:
// - SyncInterval: 1 second
// - BatchSize: 1000
// - MaxRetries: 3
// - RetryDelay: 30 seconds
// - EnableSync: true
// - All start times: Unix epoch (1970-01-01)
```

### Custom Configuration Example

```go
config := &SyncConfig{
    SyncInterval:         5 * time.Second,  // Sync every 5 seconds
    BatchSize:            500,              // Smaller batches
    MaxRetries:           5,                // More retries
    RetryDelay:           10 * time.Second, // Shorter retry delay
    EnableSync:           true,
    TransactionStartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
}
```

### Environment Variables

The sync system requires the following environment variable:

- **`DB_URL`**: PostgreSQL connection string (required)
  - Format: `postgres://user:password@host:port/database?sslmode=disable`
  - Example: `postgres://blnk:password@localhost:5432/blnk?sslmode=disable`

---

## Usage

### Basic Usage

```go
package main

import (
    "log"
    "time"
    "watch"
)

func main() {
    // Initialize DuckDB first
    if err := watch.InitTransactionsDB(); err != nil {
        log.Fatal(err)
    }
    defer watch.CloseTransactionsDB()

    // Create syncer with default config
    syncer := watch.NewWatermarkSyncer(nil)

    // Start the sync process
    if err := syncer.Start(); err != nil {
        log.Fatal(err)
    }

    // Your application continues running...
    // Sync happens in background

    // Stop when done
    defer syncer.Stop()
}
```

### Custom Configuration

```go
// Create custom configuration
config := &watch.SyncConfig{
    SyncInterval: 10 * time.Second,
    BatchSize:    2000,
    MaxRetries:   5,
    RetryDelay:   15 * time.Second,
    EnableSync:   true,
    
    // Start from a specific date
    TransactionStartTime: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
}

// Create syncer with custom config
syncer := watch.NewWatermarkSyncer(config)

// Start sync
if err := syncer.Start(); err != nil {
    log.Fatal(err)
}
```

### Checking Sync Status

```go
// Get current sync status
status, err := syncer.GetSyncStatus()
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Transactions synced: %d\n", status.TotalSyncedCount)
fmt.Printf("Sync status: %s\n", status.SyncStatus)
fmt.Printf("Last transaction sync: %s\n", status.LastSyncTimestamp)
```

### Manual Sync Trigger

```go
// Force an immediate sync (useful for testing or manual triggers)
if err := syncer.ForceSync(); err != nil {
    log.Printf("Force sync failed: %v", err)
}
```

### Resetting Watermark

```go
// Reset watermark to start from beginning (or config start times)
// WARNING: This will cause all data to be re-synced!
if err := syncer.ResetWatermark(); err != nil {
    log.Printf("Reset failed: %v", err)
}
```

---

## API Reference

### WatermarkSyncer

#### `NewWatermarkSyncer(config *SyncConfig) *WatermarkSyncer`

Creates a new watermark syncer instance.

**Parameters**:
- `config`: Sync configuration. If `nil`, uses default configuration.

**Returns**: `*WatermarkSyncer` instance

**Example**:
```go
syncer := watch.NewWatermarkSyncer(nil)
```

---

#### `Start() error`

Starts the background sync loop. The sync will run periodically based on `SyncInterval`.

**Returns**: 
- `error`: Returns error if sync is already running or if sync fails to start

**Example**:
```go
if err := syncer.Start(); err != nil {
    log.Fatal(err)
}
```

---

#### `Stop()`

Stops the background sync loop gracefully.

**Example**:
```go
syncer.Stop()
```

---

#### `GetSyncStatus() (*SyncWatermark, error)`

Retrieves the current sync status and watermark information.

**Returns**:
- `*SyncWatermark`: Current watermark state
- `error`: Error if status cannot be retrieved

**Example**:
```go
status, err := syncer.GetSyncStatus()
if err != nil {
    log.Fatal(err)
}
```

---

#### `ForceSync() error`

Manually triggers an immediate sync operation, bypassing the normal interval.

**Returns**:
- `error`: Error if sync fails

**Example**:
```go
if err := syncer.ForceSync(); err != nil {
    log.Printf("Sync failed: %v", err)
}
```

---

#### `ResetWatermark() error`

Resets the watermark to the configured start times, effectively restarting the sync from the beginning.

**Warning**: This will cause all data to be re-synced on the next sync cycle!

**Returns**:
- `error`: Error if reset fails

**Example**:
```go
if err := syncer.ResetWatermark(); err != nil {
    log.Printf("Reset failed: %v", err)
}
```

---

### SyncWatermark Structure

```go
type SyncWatermark struct {
    ID                  int       `json:"id"`
    LastSyncTimestamp   time.Time `json:"last_sync_timestamp"`
    LastTransactionID   string    `json:"last_transaction_id,omitempty"`
    TotalSyncedCount    int64     `json:"total_synced_count"`
    LastSyncCompletedAt time.Time `json:"last_sync_completed_at"`
    SyncStatus          string    `json:"sync_status"` // "idle", "running", "failed"
    CreatedAt           time.Time `json:"created_at"`
    UpdatedAt           time.Time `json:"updated_at"`
}
```

---

## Examples

### Example 1: Basic Setup with Defaults

```go
package main

import (
    "log"
    "os"
    "watch"
)

func main() {
    // Set required environment variable
    os.Setenv("DB_URL", "postgres://user:pass@localhost:5432/blnk?sslmode=disable")
    
    // Initialize database
    if err := watch.InitTransactionsDB(); err != nil {
        log.Fatal(err)
    }
    defer watch.CloseTransactionsDB()
    
    // Create and start syncer
    syncer := watch.NewWatermarkSyncer(nil)
    if err := syncer.Start(); err != nil {
        log.Fatal(err)
    }
    defer syncer.Stop()
    
    // Application continues...
    select {} // Block forever
}
```

### Example 2: Custom Configuration for High-Volume System

```go
package main

import (
    "log"
    "time"
    "watch"
)

func main() {
    // Configure for high-volume system
    config := &watch.SyncConfig{
        SyncInterval: 30 * time.Second,  // Less frequent syncs
        BatchSize:    5000,              // Larger batches
        MaxRetries:   10,                // More retries for reliability
        RetryDelay:   5 * time.Second,   // Quick retries
        EnableSync:   true,
    }

    syncer := watch.NewWatermarkSyncer(config)

    if err := syncer.Start(); err != nil {
        log.Fatal(err)
    }
    defer syncer.Stop()

    // Monitor sync status periodically
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()

    for range ticker.C {
        status, err := syncer.GetSyncStatus()
        if err != nil {
            log.Printf("Error getting status: %v", err)
            continue
        }

        log.Printf("Sync Status: %s", status.SyncStatus)
        log.Printf("Transactions: %d", status.TotalSyncedCount)
    }
}
```

### Example 3: Starting from a Specific Date

```go
package main

import (
    "log"
    "time"
    "watch"
)

func main() {
    // Start syncing from January 1, 2024
    startDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

    config := &watch.SyncConfig{
        SyncInterval:         5 * time.Second,
        BatchSize:            1000,
        MaxRetries:           3,
        RetryDelay:           30 * time.Second,
        EnableSync:           true,
        TransactionStartTime: startDate,
    }

    syncer := watch.NewWatermarkSyncer(config)

    if err := syncer.Start(); err != nil {
        log.Fatal(err)
    }
    defer syncer.Stop()

    // Wait for initial sync
    time.Sleep(10 * time.Second)

    // Check status
    status, err := syncer.GetSyncStatus()
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Initial sync completed. Synced %d transactions", status.TotalSyncedCount)
}
```

### Example 4: Graceful Shutdown

```go
package main

import (
    "log"
    "os"
    "os/signal"
    "syscall"
    "watch"
)

func main() {
    syncer := watch.NewWatermarkSyncer(nil)
    
    if err := syncer.Start(); err != nil {
        log.Fatal(err)
    }
    
    // Setup graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    
    // Wait for shutdown signal
    <-sigChan
    log.Println("Shutting down...")
    
    // Stop syncer gracefully
    syncer.Stop()
    
    log.Println("Shutdown complete")
}
```

---

## Troubleshooting

### Common Issues

#### 1. "DB_URL environment variable is required"

**Problem**: The `DB_URL` environment variable is not set.

**Solution**: Set the environment variable before starting the syncer:
```go
os.Setenv("DB_URL", "postgres://user:pass@host:port/db?sslmode=disable")
```

---

#### 2. "Failed to install/load PostgreSQL extension"

**Problem**: DuckDB cannot load the PostgreSQL extension.

**Solution**: 
- Ensure DuckDB is properly installed
- Check that the PostgreSQL extension is available
- Verify network connectivity to PostgreSQL database

---

#### 3. "Failed to attach PostgreSQL database"

**Problem**: Cannot connect to the PostgreSQL database.

**Solution**:
- Verify `DB_URL` is correct
- Check PostgreSQL is running and accessible
- Verify credentials and permissions
- Check firewall/network settings

---

#### 4. Sync Status Stuck on "running"

**Problem**: Sync status remains "running" even after sync completes.

**Solution**: This may indicate a crash during sync. Check logs and consider:
- Manually updating sync status
- Resetting watermark if needed
- Checking for database connection issues

---

#### 5. No Records Being Synced

**Problem**: Sync runs but no records are copied.

**Possible Causes**:
- Watermark is ahead of all records (check `LastSyncTimestamp`)
- All records already exist in DuckDB
- Records don't meet sync criteria (e.g., status is QUEUED for transactions)

**Solution**:
- Check watermark status: `syncer.GetSyncStatus()`
- Verify records exist in PostgreSQL with timestamps after watermark
- Check sync logs for errors

---

### Debugging Tips

1. **Enable Debug Logging**: Set log level to debug to see detailed sync information
2. **Check Watermark State**: Use `GetSyncStatus()` to inspect current watermark
3. **Monitor Sync Logs**: Watch for error messages in sync logs
4. **Test Connection**: Verify PostgreSQL connection before starting sync
5. **Force Sync**: Use `ForceSync()` to test sync manually

---

## Best Practices

### 1. Configuration

- **Batch Size**: 
  - Use larger batches (2000-5000) for high-volume systems
  - Use smaller batches (500-1000) for low-latency requirements
- **Sync Interval**: 
  - Balance between freshness and database load
  - Typical: 5-30 seconds for most use cases
- **Retry Settings**: 
  - Increase `MaxRetries` for unreliable networks
  - Adjust `RetryDelay` based on expected recovery time

### 2. Starting Timestamps

- **Initial Setup**: Use Unix epoch (1970-01-01) to sync all historical data
- **Incremental Setup**: Use a recent date to only sync new data
- **Recovery**: Use a date before the issue to re-sync missing data

### 3. Monitoring

- **Regular Status Checks**: Monitor `GetSyncStatus()` periodically
- **Alert on Failures**: Set up alerts when sync status is "failed"
- **Track Metrics**: Monitor `TotalSyncedCount` to ensure sync is progressing

### 4. Error Handling

- **Graceful Degradation**: Sync continues even if one table fails
- **Retry Logic**: Configure appropriate retry settings
- **Logging**: Ensure comprehensive logging for troubleshooting

### 5. Performance

- **Database Indexes**: Ensure proper indexes on `created_at` and ID fields in PostgreSQL
- **Connection Pooling**: DuckDB handles connections internally
- **Batch Processing**: Use appropriate batch sizes to balance memory and performance

### 6. Data Integrity

- **Idempotency**: Sync is idempotent - safe to run multiple times
- **Deduplication**: Uses `INSERT OR REPLACE` to handle duplicates
- **Transaction Safety**: Each sync cycle is independent

---

## Database Schema

### sync_watermark Table

The watermark state is stored in the `sync_watermark` table:

```sql
CREATE TABLE sync_watermark (
    id INTEGER PRIMARY KEY DEFAULT 1,
    last_sync_timestamp TIMESTAMP,
    last_transaction_id VARCHAR,
    total_synced_count BIGINT DEFAULT 0,
    last_sync_completed_at TIMESTAMP,
    sync_status VARCHAR DEFAULT 'idle',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (id = 1)
);
```

---

## Summary

The Watermark Sync system provides:

✅ **Efficient incremental synchronization** from PostgreSQL to DuckDB
✅ **Automatic retry logic** for fault tolerance
✅ **Configurable sync intervals** and batch sizes
✅ **Transaction sync** for payment and transaction records
✅ **Resumable sync** with watermark tracking
✅ **Status monitoring** and debugging capabilities

Use this system when you need to keep a local DuckDB database synchronized with a remote PostgreSQL database for analytics, reporting, or watch rule evaluation.
