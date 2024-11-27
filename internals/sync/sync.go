package sync

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/JakubPluta/go-rs-pg-sync/internals/db"
)

// SyncDirection represents the direction of the sync operation
type SyncDirection string

const (
	RedshiftToPostgres SyncDirection = "redshift_to_postgres"
	// PostgresToRedshift SyncDirection = "postgres_to_redshift"  // TODO
)

// TableMapping represents the mapping between source and target tables
type TableMapping struct {
	SourceTable    string
	TargetTable    string
	ColumnsMapping map[string]string // source column -> target column e.g { "id": "id", "name": "big_name" }
	ExcludeColumns []string          // columns to exclude
	BatchSize      int
	Concurrency    int
}

// SyncConfig represents the configuration for the sync operation
type SyncConfig struct {
	Source           *db.Config
	Target           *db.Config
	Direction        SyncDirection
	DefaultBatchSize int
	MaxConcurrency   int

	Tables        []TableMapping
	RetryAttempts int
	RetryDelay    int
}

// SyncStats holds statistics about the synchronization process
type SyncStats struct {
	StartTime          time.Time
	EndTime            time.Time
	TableName          string
	TotalRows          int64
	ProcessedRows      int64
	FailedRows         int64
	RetryCount         int
	BatchesProcessed   int
	BatchesFailed      int
	AverageBatchTime   time.Duration
	TotalExecutionTime time.Duration
}

// Validate checks if the configuration is valid.
//
// Validation rules:
// - Source configuration is required
// - Target configuration is required
// - At least one table mapping is required
// - Default batch size must be positive
// - Max concurrency must be positive
//
// Returns an error if the configuration is invalid. Otherwise returns nil.
func (c *SyncConfig) Validate() error {
	if c.Source == nil {
		return fmt.Errorf("%w: source configuration is required", db.ErrInvalidConfig)
	}
	if c.Target == nil {
		return fmt.Errorf("%w: target configuration is required", db.ErrInvalidConfig)
	}
	if len(c.Tables) == 0 {
		return fmt.Errorf("%w: at least one table mapping is required", db.ErrInvalidConfig)
	}
	if c.DefaultBatchSize <= 0 {
		return fmt.Errorf("%w: default batch size must be positive", db.ErrInvalidConfig)
	}
	if c.MaxConcurrency <= 0 {
		return fmt.Errorf("%w: max concurrency must be positive", db.ErrInvalidConfig)
	}
	return nil
}

// DatabaseSyncer represents a database sync operation. It is responsible for synchronizing data between two databases
type DatabaseSyncer struct {
	source db.DatabaseClient
	target db.DatabaseClient
	config *SyncConfig
	logger *log.Logger
	stats  map[string]*SyncStats // table name -> stats
	mu     sync.RWMutex          // mutex for synchronization
}

// NewDatabaseSyncer creates a new DatabaseSyncer instance from the given parameters.
//
// source and target are the source and target databases respectively.
// config is the configuration for the sync operation.
// logger is the logger for the sync operation. If nil, a new logger will be created.
//
// It returns a new DatabaseSyncer instance and an error, which is non-nil if the configuration is invalid.
func NewDatabaseSyncer(source, target db.DatabaseClient, config *SyncConfig, logger *log.Logger) (*DatabaseSyncer, error) {
	if logger == nil {
		logger = log.New(log.Writer(), "[DB-SYNC] ", log.LstdFlags|log.Lmicroseconds)
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &DatabaseSyncer{
		source: source,
		target: target,
		config: config,
		logger: logger,
		stats:  make(map[string]*SyncStats),
	}, nil
}

// buildSourceQuery constructs a SQL SELECT query for the source table based on the provided
// table mapping. It maps the source columns to the target columns and excludes any columns
// specified in the ExcludeColumns list of the TableMapping. If the ColumnsMapping is nil,
// an empty query string will be returned.
//
// Parameters:
//   - tableMapping: The mapping that specifies the source and target columns, and any columns
//     to exclude from the query.
//
// Returns:
// A SQL SELECT query string that selects the mapped columns from the source table.
func (s *DatabaseSyncer) buildSourceQuery(tableMapping TableMapping) string {
	if tableMapping.ColumnsMapping == nil {
		// If ColumnsMapping is nil, return an empty query
		return fmt.Sprintf("SELECT * FROM %s", tableMapping.SourceTable)
	}
	columns := make([]string, 0)
	for sourceCol, targetCol := range tableMapping.ColumnsMapping {
		if !contains(tableMapping.ExcludeColumns, sourceCol) {
			columns = append(columns, fmt.Sprintf("%s AS %s", sourceCol, targetCol))
		}
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), tableMapping.SourceTable)
	return query
}

// Helper functions

// quoteIdentifiers takes a slice of string identifiers and returns a new slice
// where each identifier is wrapped in double quotes.
//
// Parameters:
//   - identifiers: A slice of strings representing the identifiers to be quoted.
//
// Returns:
// A new slice of strings where each identifier from the input slice is surrounded
// by double quotes.
func quoteIdentifiers(identifiers []string) []string {
	quoted := make([]string, len(identifiers))
	for i, id := range identifiers {
		quoted[i] = fmt.Sprintf(`"%s"`, id)
	}
	return quoted
}

// contains checks if a given string is present in a given slice of strings.
//
// It iterates over the slice and returns true if the string is found, false otherwise.
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// join takes a slice of strings and joins them together with the given separator string.
//
// If the slice is empty, an empty string is returned.
//
// Parameters:
//   - slice: The slice of strings to join
//   - sep: The separator string to use when joining the strings
//
// Returns:
// The joined string
func join(slice []string, sep string) string {
	if len(slice) == 0 {
		return ""
	}
	result := slice[0]
	for _, s := range slice[1:] {
		result += sep + s
	}
	return result
}
