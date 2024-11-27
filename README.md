# Database Synchronization Tool
Tool for synchronizing data between Amazon Redshift and PostgreSQL databases. This project supports large-scale data transfers, ensuring data consistency and offering customizable table mappings with retry and concurrency controls.

### Features

#### Bidirectional Synchronization:

Currently supports synchronization from Amazon Redshift to PostgreSQL.
(Future support planned for PostgreSQL to Redshift).

#### Customizable Table Mappings:

- Define specific source-to-target table mappings.
- Rename or map source columns to target columns.
- Exclude unwanted columns from synchronization.
- Set batch size and concurrency limits per table.

#### High Performance:

- Handles large datasets with configurable batch processing.
- Supports concurrency for parallel processing of data chunks.

#### Resilient Synchronization:

- Retry logic with customizable retry attempts and delays.
- Tracks synchronization statistics for each table.


### Installation

Prerequisites
- Go 1.18+ installed.
- Access to Amazon Redshift and PostgreSQL databases.
- Environment variables configured for both source and target databases.

Clone the Repository
```bash
git clone https://github.com/JakubPluta/go-rs-pg-sync.git
cd go-rs-pg-sync
```

Install Dependencies

```bash
go mod tidy
```

#### Configuration

Set the following environment variables for your source (Redshift) and target (PostgreSQL) databases:

| Variable           | Description                 |
|--------------------|-----------------------------|
| `REDSHIFT_HOST`    | Redshift database host.     |
| `REDSHIFT_DB`      | Redshift database name.     |
| `REDSHIFT_USER`    | Redshift username.          |
| `REDSHIFT_PASSWORD`| Redshift password.          |
| `PG_HOST`          | PostgreSQL database host.   |
| `PG_DB`            | PostgreSQL database name.   |
| `PG_USER`          | PostgreSQL username.        |
| `PG_PASSWORD`      | PostgreSQL password.        |



#### Usage
Define Table Mappings
In the main.go file, customize the table mappings as needed:

```go
tablesMappings := []sync.TableMapping{
    {
        SourceTable: "users",
        TargetTable: "users",
        BatchSize:   1000,
        Concurrency: 4,
        ColumnsMapping: map[string]string{
            "user_id":    "id",
            "user_name":  "name",
            "email":      "email",
            "created_at": "created_at",
            "updated_at": "updated_at",
            "status":     "status",
            "last_login": "last_login",
        },
        ExcludeColumns: []string{"temp_field", "internal_id"},
    },
}
```

#### Run the Synchronization

```bash
go run main.go
```

### Logging and Statistics
The synchronization process logs detailed information:

- Queries executed for each table.
- Errors encountered and retry attempts.
- Synchronization statistics such as rows processed, failed, and average batch time.


Example log output:

```less
[SYNC] Starting database synchronization...
[SYNC] Syncing table: users
[SYNC] Source query: SELECT user_id AS id, user_name AS name, ...
[SYNC] Target query: INSERT INTO users ("id","name",...) VALUES ($1,$2,...)
[SYNC] Synchronization completed successfully in 1m23.456s
```

```go

syncConfig := &sync.SyncConfig{
    Source:           sourceConfig,
    Target:           targetConfig,
    Direction:        sync.RedshiftToPostgres,
    DefaultBatchSize: 1000,
    MaxConcurrency:   5,
    RetryAttempts:    3,
    RetryDelay:       5, // seconds
    Tables:           tablesMappings,
}
```

### Context Management
The sync process can run for a limited duration by using a context with a timeout:

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
defer cancel()
```