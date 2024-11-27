package sync

import (
	"context"
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
	ColumnsMapping map[string]string // Optional source column -> target column e.g { "id": "id", "name": "big_name" }
	ExcludeColumns []string          // Optional columns to exclude
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

// buildTargetQuery constructs a SQL INSERT query for the target table based on the provided
// table mapping and a chunk of data. It maps the source columns to the target columns, excludes
// any columns specified in the ExcludeColumns list of the TableMapping, and prepares the
// placeholders for the values.
//
// Parameters:
//   - tableMapping: The mapping that specifies the source and target columns, and any columns
//     to exclude from the query.
//   - chunk: A slice of rows, where each row is a map of column names to values.
//
// Returns:
// A tuple containing:
//   - A SQL INSERT query string that inserts the mapped columns into the target table.
//   - A slice of interface{} containing the values to be inserted, which correspond to the
//     placeholders in the query.
func (s *DatabaseSyncer) buildTargetQuery(tableMapping TableMapping, chunk []db.Row) (string, []interface{}) {
	if len(chunk) == 0 {
		// If the chunk is empty, return an empty query
		return "", nil
	}

	// Prepare columns to insert
	var columns []string
	if len(tableMapping.ColumnsMapping) > 0 {
		for _, targetCol := range tableMapping.ColumnsMapping {
			if !contains(tableMapping.ExcludeColumns, targetCol) {
				columns = append(columns, targetCol)
			}
		}
	} else {
		// If ColumnsMapping is nil, insert all columns
		for col := range chunk[0] {
			if !contains(tableMapping.ExcludeColumns, col) {
				columns = append(columns, col)
			}
		}
	}

	// Prepare placeholders for values
	valueStrings := make([]string, 0, len(chunk))
	valueArgs := make([]interface{}, 0, len(chunk)*len(columns))

	for i, row := range chunk {
		// Prepare placeholders
		placeholders := make([]string, len(columns))
		for j, col := range columns {
			valueArgs = append(valueArgs, row[col])
			placeholders[j] = fmt.Sprintf("$%d", i*len(columns)+j+1)
		}
		valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(placeholders, ",")))
	}

	// Build query with insert placeholders
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tableMapping.TargetTable,
		strings.Join(quoteIdentifiers(columns), ","),
		strings.Join(valueStrings, ","),
	)

	return query, valueArgs
}

// SyncTable synchronizes data from a source table to a target table based on the provided
// table mapping. It constructs queries to fetch data from the source and insert it into
// the target, handling batch processing and concurrency.
//
// Parameters:
//   - ctx: The context for managing request lifecycle and cancellation.
//   - tableMapping: The mapping that specifies the source and target tables, column mappings,
//     and other sync-specific settings.
//
// Returns:
// An error if the synchronization process fails at any step, otherwise returns nil.
//
// The method logs the start and end of the table synchronization process, and updates sync
// statistics, including processed rows, failed rows, and execution time. It also handles
// retries for failed insert operations based on the configuration.
func (s *DatabaseSyncer) SyncTable(ctx context.Context, tableMapping TableMapping) error {
	s.logger.Printf("Syncing table: %s", tableMapping.TargetTable)

	stats := &SyncStats{
		StartTime: time.Now(),
		TableName: tableMapping.TargetTable,
	}

	s.mu.Lock() // Lock mutex before updating stats
	s.stats[tableMapping.TargetTable] = stats
	s.mu.Unlock() // Unlock mutex after updating stats
	// Set batch size
	batchSize := tableMapping.BatchSize
	if batchSize <= 0 {
		batchSize = s.config.DefaultBatchSize
	}
	sourceQuery := s.buildSourceQuery(tableMapping)
	s.logger.Printf("Source query: %s", sourceQuery)

	// Fetch data from source
	chunks, err := s.source.FetchChunks(ctx, sourceQuery, batchSize)
	if err != nil {
		return fmt.Errorf("failed to fetch data from source: %w", err)
	}

	// set up error channel
	errChan := make(chan error, len(chunks))
	var wg sync.WaitGroup

	// define semaphore for max concurrency limit
	semaphore := make(chan struct{}, tableMapping.Concurrency)

	for i, chunk := range chunks {
		wg.Add(1)               // Increment waitgroup counter
		semaphore <- struct{}{} // Acquire semaphore
		go func(chunkIdx int, dataChunk []db.Row) {
			defer wg.Done()                // Decrement waitgroup counter
			defer func() { <-semaphore }() // Release semaphore

			startTime := time.Now()

			query, args := s.buildTargetQuery(tableMapping, dataChunk)
			s.logger.Printf("Target query: %s", query)
			var insertErr error

			for attempt := 0; attempt < s.config.RetryAttempts; attempt++ {
				if attempt > 0 {
					s.logger.Printf("Retrying after error: %v", insertErr)
					time.Sleep(time.Duration(s.config.RetryDelay) * time.Second)
				}

				if _, err := s.target.Query(ctx, query, args...); err != nil {
					s.mu.Lock() // Lock mutex before updating stats
					stats.ProcessedRows += int64(len(dataChunk))
					stats.BatchesProcessed++
					stats.AverageBatchTime = (stats.AverageBatchTime*time.Duration(stats.BatchesProcessed-1) +
						time.Since(startTime)) / time.Duration(stats.BatchesProcessed)
					s.mu.Unlock()
					return
				} else {
					insertErr = err
					s.logger.Printf("Retry %d failed for chunk %d: %v", attempt+1, chunkIdx, err)
				}
			}
			// if all retries failed, return error
			s.mu.Lock() // Lock mutex before updating stats
			stats.FailedRows += int64(len(dataChunk))
			stats.BatchesFailed++
			s.mu.Unlock()
			errChan <- fmt.Errorf("failed to insert chunk %d: %w", chunkIdx, insertErr)
		}(i, chunk)

	}

	wg.Wait()
	close(errChan)

	// collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}
	s.mu.Lock()
	stats.EndTime = time.Now()
	stats.TotalExecutionTime = stats.EndTime.Sub(stats.StartTime)
	s.mu.Unlock()
	if len(errors) > 0 {
		return fmt.Errorf("sync completed with %d errors: %v", len(errors), errors)
	}

	s.logger.Printf("Successfully synced table %s -> %s. Stats: %+v",
		tableMapping.SourceTable, tableMapping.TargetTable, stats)
	return nil

}

// Sync synchronizes data between the source and target databases based on the configuration.
//
// It iterates over the configured table mappings and calls SyncTable for each mapping.
// If any of the calls to SyncTable fail, an error is returned.
//
// The function returns an error if any of the table syncs fail. Otherwise, it returns nil.
func (s *DatabaseSyncer) Sync(ctx context.Context) error {
	for _, mapping := range s.config.Tables {
		if err := s.SyncTable(ctx, mapping); err != nil {
			return fmt.Errorf("failed to sync table %s: %w", mapping.SourceTable, err)
		}
	}
	return nil
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
