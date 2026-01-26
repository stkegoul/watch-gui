# Blnk Watch API Documentation

## Overview

The Blnk Watch service provides a REST API for managing transaction monitoring rules, injecting transactions for evaluation, and managing watch instructions. The service runs on **port 8081** by default.

**Base URL**: `http://localhost:8081`

---

## Table of Contents

1. [Transaction APIs](#transaction-apis)
2. [Instruction Management APIs](#instruction-management-apis)
3. [Git Repository APIs](#git-repository-apis)
4. [Data Models](#data-models)
5. [Error Handling](#error-handling)

---

## Transaction APIs

### 1. Inject Transaction

Injects a transaction into the watch system for real-time risk evaluation.

**Endpoint**: `POST /inject`

**Request Body**:
```json
{
  "transaction_id": "string (optional, auto-generated if not provided)",
  "amount": 1000.50,
  "currency": "USD",
  "source": "balance_id_123",
  "destination": "balance_id_456",
  "reference": "unique_reference_string",
  "description": "Payment for services",
  "status": "pending",
  "hash": "transaction_hash",
  "allow_overdraft": false,
  "inflight": false,
  "skip_queue": false,
  "atomic": false,
  "created_at": "2024-01-15T10:30:00Z",
  "effective_date": "2024-01-15T10:30:00Z",
  "scheduled_for": "2024-01-15T10:30:00Z",
  "inflight_expiry_date": "2024-01-15T11:30:00Z",
  "meta_data": {
    "key1": "value1",
    "key2": 123,
    "nested": {
      "field": "value"
    }
  }
}
```

**Required Fields**:
- `amount`: Transaction amount (float64)
- `currency`: Currency code (string)
- `reference`: Unique reference identifier (string)

**Optional Fields**:
- `transaction_id`: Auto-generated UUID if not provided
- `created_at`: Defaults to current time if not provided
- `source`, `destination`: Balance identifiers
- `description`: Transaction description
- `status`: Transaction status
- `meta_data`: Additional metadata as key-value pairs

**Response**:
- **Status Code**: `200 OK`
- **Body**: Empty (transaction processed successfully)

**Example Request**:
```bash
curl -X POST http://localhost:8081/inject \
  -H "Content-Type: application/json" \
  -d '{
    "amount": 5000.00,
    "currency": "USD",
    "source": "balance_123",
    "destination": "balance_456",
    "reference": "txn_ref_001",
    "description": "High-value transfer",
    "meta_data": {
      "merchant_category": "finance",
      "country": "US"
    }
  }'
```

---

### 2. Blnk Webhook

Receives webhook events from Blnk and processes transactions automatically.

**Endpoint**: `POST /blnkwebhook`

**Request Body**:
```json
{
  "event": "transaction.created",
  "data": {
    "transaction_id": "txn_123",
    "amount": 1000.00,
    "currency": "USD",
    "source": "balance_123",
    "destination": "balance_456",
    "reference": "ref_001",
    "status": "applied",
    "created_at": "2024-01-15T10:30:00Z",
    "meta_data": {
      "key": "value"
    }
  }
}
```

**Response**:
- **Status Code**: `200 OK`
- **Body**: `"Transaction {transaction_id} from webhook event '{event}' processed successfully"`

**Example Request**:
```bash
curl -X POST http://localhost:8081/blnkwebhook \
  -H "Content-Type: application/json" \
  -d '{
    "event": "transaction.created",
    "data": {
      "transaction_id": "txn_123",
      "amount": 1000.00,
      "currency": "USD",
      "source": "balance_123",
      "destination": "balance_456",
      "reference": "ref_001"
    }
  }'
```

---

### 3. Get Transaction by ID

Retrieves a transaction by its transaction ID.

**Endpoint**: `GET /transactions/{transaction_id}`

**Path Parameters**:
- `transaction_id` (string, required): The transaction ID to retrieve

**Response**:
- **Status Code**: `200 OK`
- **Body**: Transaction object (same structure as inject request)

**Example Request**:
```bash
curl http://localhost:8081/transactions/txn_123
```

**Example Response**:
```json
{
  "transaction_id": "txn_123",
  "amount": 1000.00,
  "currency": "USD",
  "source": "balance_123",
  "destination": "balance_456",
  "reference": "ref_001",
  "description": "Payment",
  "status": "applied",
  "created_at": "2024-01-15T10:30:00Z",
  "meta_data": {
    "key": "value"
  }
}
```

**Error Responses**:
- `404 Not Found`: Transaction not found
- `500 Internal Server Error`: Failed to retrieve transaction

---

## Instruction Management APIs

### 1. List All Instructions

Retrieves all watch instructions/rules.

**Endpoint**: `GET /instructions`

**Response**:
- **Status Code**: `200 OK`
- **Body**: Array of Instruction objects

**Example Request**:
```bash
curl http://localhost:8081/instructions
```

**Example Response**:
```json
[
  {
    "id": 1,
    "name": "HighValueTransaction",
    "text": "rule HighValueTransaction {\n    description \"Review any transaction above $10,000\"\n    when amount > 10000\n    then review\n         score   0.5\n         reason  \"Amount exceeds threshold\"\n}",
    "description": "Review any transaction above $10,000",
    "dsl_json": "{\"name\":\"HighValueTransaction\",\"when\":[...],\"then\":{...}}",
    "created_at": "2024-01-15T10:00:00Z",
    "updated_at": "2024-01-15T10:00:00Z"
  }
]
```

---

### 2. Get Instruction by ID

Retrieves a specific instruction by its ID.

**Endpoint**: `GET /instructions/{id}`

**Path Parameters**:
- `id` (integer, required): The instruction ID

**Response**:
- **Status Code**: `200 OK`
- **Body**: Instruction object

**Example Request**:
```bash
curl http://localhost:8081/instructions/1
```

**Example Response**:
```json
{
  "id": 1,
  "name": "HighValueTransaction",
  "text": "rule HighValueTransaction {...}",
  "description": "Review any transaction above $10,000",
  "dsl_json": "{\"name\":\"HighValueTransaction\",...}",
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```

**Error Responses**:
- `400 Bad Request`: Invalid instruction ID format
- `404 Not Found`: Instruction not found
- `500 Internal Server Error`: Failed to retrieve instruction

---

### 3. Delete Instruction

Deletes an instruction by its ID.

**Endpoint**: `DELETE /instructions/{id}`

**Path Parameters**:
- `id` (integer, required): The instruction ID to delete

**Response**:
- **Status Code**: `204 No Content`
- **Body**: Empty

**Example Request**:
```bash
curl -X DELETE http://localhost:8081/instructions/1
```

**Error Responses**:
- `400 Bad Request`: Invalid instruction ID format
- `404 Not Found`: Instruction not found
- `500 Internal Server Error`: Failed to delete instruction

---

### 4. Compile and Save Instruction

Compiles a watch script and saves it as an instruction.

**Endpoint**: `POST /compile-and-save-instruction`

**Request Body**:
```json
{
  "script": "rule HighValueTransaction {\n    description \"Review any transaction above $10,000\"\n    when amount > 10000\n    then review\n         score   0.5\n         reason  \"Amount exceeds threshold\"\n}"
}
```

**Required Fields**:
- `script` (string): The watch script in DSL format (see README.md for syntax)

**Response**:
- **Status Code**: `201 Created`
- **Body**: Created Instruction object

**Example Request**:
```bash
curl -X POST http://localhost:8081/compile-and-save-instruction \
  -H "Content-Type: application/json" \
  -d '{
    "script": "rule HighValueTransaction {\n    description \"Review any transaction above $10,000\"\n    when amount > 10000\n    then review\n         score   0.5\n         reason  \"Amount exceeds threshold\"\n}"
  }'
```

**Example Response**:
```json
{
  "id": 1,
  "name": "HighValueTransaction",
  "text": "rule HighValueTransaction {...}",
  "description": "Review any transaction above $10,000",
  "dsl_json": "{\"name\":\"HighValueTransaction\",...}",
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```

**Error Responses**:
- `400 Bad Request`: Invalid request body or empty script
- `409 Conflict`: Instruction with the same name already exists
- `500 Internal Server Error`: Failed to compile script or save instruction

---

## Git Repository APIs

### 1. Get Git Status

Retrieves the current status of the configured Git repository.

**Endpoint**: `GET /git/status`

**Response**:
- **Status Code**: `200 OK`
- **Body**: GitStatusResponse object

**Example Request**:
```bash
curl http://localhost:8081/git/status
```

**Example Response**:
```json
{
  "configured": true,
  "repo_url": "https://github.com/user/watch-scripts.git",
  "branch": "main",
  "local_path": "watch_scripts",
  "current_commit": "abc123def456",
  "remote_commit": "abc123def456",
  "up_to_date": true,
  "last_sync": "2024-01-15T10:00:00Z",
  "error": ""
}
```

**Response Fields**:
- `configured` (boolean): Whether Git repository is configured
- `repo_url` (string): Git repository URL
- `branch` (string): Branch name
- `local_path` (string): Local directory path
- `current_commit` (string): Current local commit hash
- `remote_commit` (string): Remote commit hash
- `up_to_date` (boolean): Whether local and remote are in sync
- `last_sync` (string): Last sync timestamp
- `error` (string): Error message if any

**Example Response (Not Configured)**:
```json
{
  "configured": false,
  "error": "Git repository not configured"
}
```

---

### 2. Sync Git Repository

Manually triggers a sync operation to pull the latest changes from the Git repository.

**Endpoint**: `POST /git/sync`

**Response**:
- **Status Code**: `200 OK`
- **Body**: GitSyncResponse object

**Example Request**:
```bash
curl -X POST http://localhost:8081/git/sync
```

**Example Response**:
```json
{
  "success": true,
  "message": "Repository synced and scripts reloaded",
  "before_commit": "abc123def456",
  "after_commit": "def456ghi789",
  "scripts_loaded": 5,
  "error": ""
}
```

**Response Fields**:
- `success` (boolean): Whether sync was successful
- `message` (string): Status message
- `before_commit` (string): Commit hash before sync
- `after_commit` (string): Commit hash after sync
- `scripts_loaded` (integer): Number of scripts loaded (if changed)
- `error` (string): Error message if sync failed

**Example Response (Error)**:
```json
{
  "success": false,
  "error": "Git repository not configured"
}
```

---

## Data Models

### Transaction

```typescript
interface Transaction {
  transaction_id?: string;           // Auto-generated UUID if not provided
  amount: number;                     // Required: Transaction amount
  currency: string;                   // Required: Currency code (e.g., "USD")
  source?: string;                    // Source balance ID
  destination?: string;               // Destination balance ID
  reference: string;                  // Required: Unique reference
  description?: string;               // Transaction description
  status?: string;                    // Transaction status
  hash?: string;                      // Transaction hash
  allow_overdraft?: boolean;          // Allow overdraft
  inflight?: boolean;                 // Is inflight transaction
  skip_queue?: boolean;               // Skip queue processing
  atomic?: boolean;                   // Atomic transaction
  created_at?: string;                // ISO 8601 timestamp (defaults to now)
  effective_date?: string;           // ISO 8601 timestamp
  scheduled_for?: string;             // ISO 8601 timestamp
  inflight_expiry_date?: string;     // ISO 8601 timestamp
  meta_data?: {                       // Additional metadata
    [key: string]: any;
  };
}
```

### Instruction

```typescript
interface Instruction {
  id: number;                         // Auto-generated ID
  name: string;                       // Rule name (unique)
  text: string;                       // Original watch script text
  description?: string;               // Rule description
  dsl_json?: string;                  // Compiled DSL JSON
  created_at: string;                 // ISO 8601 timestamp
  updated_at: string;                 // ISO 8601 timestamp
}
```

### GitStatusResponse

```typescript
interface GitStatusResponse {
  configured: boolean;
  repo_url?: string;
  branch?: string;
  local_path?: string;
  current_commit?: string;
  remote_commit?: string;
  up_to_date?: boolean;
  last_sync?: string;
  error?: string;
}
```

### GitSyncResponse

```typescript
interface GitSyncResponse {
  success: boolean;
  message?: string;
  before_commit?: string;
  after_commit?: string;
  scripts_loaded?: number;
  error?: string;
}
```

---

## Error Handling

All endpoints follow standard HTTP status codes:

- **200 OK**: Request successful
- **201 Created**: Resource created successfully
- **204 No Content**: Request successful, no content to return
- **400 Bad Request**: Invalid request parameters or body
- **404 Not Found**: Resource not found
- **405 Method Not Allowed**: HTTP method not allowed for endpoint
- **409 Conflict**: Resource conflict (e.g., duplicate name)
- **500 Internal Server Error**: Server error processing request

**Error Response Format**:
```json
{
  "error": "Error message describing what went wrong"
}
```

For some endpoints, errors are returned as plain text strings in the response body.

---

## Authentication

Currently, the API does not require authentication. All endpoints are publicly accessible. In production, you should implement authentication and authorization mechanisms.

---

## Rate Limiting

Currently, there are no rate limits enforced. Consider implementing rate limiting for production use.

---

## Watch Script Syntax

For detailed information on writing watch scripts, refer to the [README.md](./README.md) file which contains comprehensive documentation on the Watch DSL syntax, examples, and best practices.

---

## Examples

### Complete Workflow Example

1. **Create a watch instruction**:
```bash
curl -X POST http://localhost:8081/compile-and-save-instruction \
  -H "Content-Type: application/json" \
  -d '{
    "script": "rule HighValueTransaction {\n    description \"Review any transaction above $10,000\"\n    when amount > 10000\n    then review\n         score   0.5\n         reason  \"Amount exceeds threshold\"\n}"
  }'
```

2. **List all instructions**:
```bash
curl http://localhost:8081/instructions
```

3. **Inject a transaction for evaluation**:
```bash
curl -X POST http://localhost:8081/inject \
  -H "Content-Type: application/json" \
  -d '{
    "amount": 15000.00,
    "currency": "USD",
    "source": "balance_123",
    "destination": "balance_456",
    "reference": "txn_001",
    "description": "Large transfer"
  }'
```

4. **Retrieve the transaction**:
```bash
curl http://localhost:8081/transactions/txn_001
```

5. **Check Git repository status**:
```bash
curl http://localhost:8081/git/status
```

6. **Sync Git repository**:
```bash
curl -X POST http://localhost:8081/git/sync
```

---

## Notes

- All timestamps should be in ISO 8601 format (e.g., `2024-01-15T10:30:00Z`)
- Transaction IDs are auto-generated as UUIDs if not provided
- Instruction names must be unique
- The service automatically evaluates transactions against all active instructions
- Git repository features require Git to be installed on the system
- Metadata fields support nested JSON structures
