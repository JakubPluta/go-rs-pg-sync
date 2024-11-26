package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

// Common errors that can be returned by the package
var (
	ErrInvalidConfig    = fmt.Errorf("invalid configuration provided")
	ErrNilConnection    = fmt.Errorf("database connection is nil")
	ErrInvalidChunkSize = fmt.Errorf("chunk size must be positive")
)

// Default Configuration Values
const (
	defaultMaxOpenConns = 10
	defaultMaxIdleConns = 5
	defaultConnTimeout  = 30 * time.Second
	defaultPort         = 5432
	defaultSSLMode      = "disable"
)

// Config encapsulates all configuration parameters needed to establish
// a connection with a Redshift database. It provides validation and
// connection string generation capabilities.
type Config struct {
	Host         string
	Port         int
	User         string
	Password     string
	Database     string
	SSLMode      string
	MaxOpenConns int
	MaxIdleConns int
	ConnTimeout  time.Duration // // Connection timeout duration
}

// NewDefaultConfig creates a new Config instance with default values.
// These values can be overridden before passing to NewRedshiftClient.
//
// Default values:
// - Port: 5439
// - SSLMode: disable
// - MaxOpenConns: 10
// - MaxIdleConns: 5
// - ConnTimeout: 30 seconds
func NewDefaultConfig() *Config {
	return &Config{
		Port:         defaultPort,
		SSLMode:      defaultSSLMode,
		MaxOpenConns: defaultMaxOpenConns,
		MaxIdleConns: defaultMaxIdleConns,
		ConnTimeout:  defaultConnTimeout,
	}
}

// DNS generates a connection string in DNS format (i.e. postgres://user:pass@host:port/dbname?sslmode=mode)
// from the given configuration.
func (c *Config) DNS() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode)
}

// Validate checks if the configuration is valid.
//
// Validation rules:
// - Host must not be empty
// - Port must be a positive integer
// - User must not be empty
// - Database must not be empty
// - MaxOpenConns must be >= MaxIdleConns
//
// Returns an error if the configuration is invalid. Otherwise returns nil.
func (c *Config) Validate() error {
	switch {
	case c.Host == "":
		return fmt.Errorf("%w: host cannot be empty", ErrInvalidConfig)
	case c.Port <= 0:
		return fmt.Errorf("%w: invalid port number", ErrInvalidConfig)
	case c.User == "":
		return fmt.Errorf("%w: user cannot be empty", ErrInvalidConfig)
	case c.Database == "":
		return fmt.Errorf("%w: database cannot be empty", ErrInvalidConfig)
	case c.MaxOpenConns < c.MaxIdleConns:
		return fmt.Errorf("%w: maxOpenConns must be >= maxIdleConns", ErrInvalidConfig)
	}
	return nil
}

// Row represents a single database row as a map of column names to values.
// Values can be of any type supported by the database driver.
type Row map[string]interface{}

// DatabaseClient is an interface that encapsulates the necessary methods
// to interact with a database. Specifically, it provides methods to query
// the database, fetch chunks of data, close the connection and ping the
// connection.
type DatabaseClient interface {
	// Query executes a query on the database with the given arguments and
	// returns the result as a *sql.Rows. sql.Rows its cursor starts before
	// the first row. If the query fails, an error is returned.
	Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// FetchChunks executes a query on the database with the given arguments
	// and returns the result as a slice of slices of maps. Each inner slice
	// represents a chunk of data and each map represents a row in the chunk.
	// The size of each chunk is determined by the chunkSize parameter. If the
	// query fails, an error is returned.
	FetchChunks(ctx context.Context, query string, chunkSize int, args ...interface{}) ([][]map[string]interface{}, error)

	// Close closes the connection to the database.
	Close() error

	// Ping checks if the connection to the database is still alive,
	// establishing a connection if necessary. If the connection is alive,
	// the method returns nil. Otherwise, an error is returned.
	Ping(ctx context.Context) error
}

// RedshiftDatabaseClient implements the DatabaseClient interface for a Redshift database.
// It provides methods to query the database, fetch chunks of data, close the connection
// and ping the connection.
type RedshiftDatabaseClient struct {
	db     *sql.DB
	logger *log.Logger
}

// Query executes a query on the database with the given arguments and
// returns the result as a *sql.Rows. sql.Rows its cursor starts before
// the first row. If the query fails, an error is returned.
func (r *RedshiftDatabaseClient) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if r.db == nil {
		// Return an error if the database connection is nil. It can happen if the database client is closed
		return nil, ErrNilConnection
	}

	r.logger.Printf("executing query: %s with args: %v", query, args)
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		// Return an error if the query fails
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

// FetchChunks executes a query on the database with the given arguments and
// returns the result as a slice of slices of maps. Each inner slice represents a chunk of data and each map represents a row in the chunk.
// The size of each chunk is determined by the chunkSize parameter. If the query fails, an error is returned.
func (r *RedshiftDatabaseClient) FetchChunks(ctx context.Context, query string, chunkSize int, args ...interface{}) ([][]Row, error) {
	if chunkSize <= 0 {
		return nil, ErrInvalidChunkSize
	}
	rows, err := r.Query(ctx, query, args...)
	if err != nil {
		// Return an error if the query fails
		return nil, err
	}
	defer rows.Close() // deferred close to ensure that the rows are closed when the function returns

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	// Prealocate slices for better perf
	results := make([][]Row, 0)        // preallocate slice of size 0 for results
	chunk := make([]Row, 0, chunkSize) // preallocate slice of size chunkSize

	// Prepare Scan destinations
	// values -> slice of pointers
	// scanArgs -> slice of values
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		// create slice for values
		// scan values into slice

		if err := ctx.Err(); err != nil {
			// Return an error if the context is canceled
			return nil, fmt.Errorf("context canceled: %w", err)
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// create map for the rows
		row := make(Row, len(columns))
		for i, col := range columns {
			row[col] = convertValueToGoType(values[i]) // convert value to go type
		}
		// add row to chunk
		chunk = append(chunk, row)
		// check if chunk is full, if it's true add it to results
		if len(chunk) == chunkSize {
			results = append(results, chunk)
			chunk = make([]Row, 0, chunkSize)
		}
	}

	if err := rows.Err(); err != nil {
		// Return an error if the rows iteration fails
		return nil, fmt.Errorf("failed to iterate rows: %v", err)
	}

	// if there are any remaingin rows
	if len(chunk) > 0 {
		results = append(results, chunk)
	}

	return results, nil

}

// Close closes the connection to the Redshift database.
// If the database connection is nil, it returns ErrNilConnection.
// Otherwise, it attempts to close the connection and returns any error encountered.
func (r *RedshiftDatabaseClient) Close() error {
	if r.db == nil {
		return ErrNilConnection
	}
	return r.db.Close()
}

// Ping checks if the connection to the database is still alive,
// establishing a connection if necessary. If the connection is alive,
// the method returns nil. Otherwise, an error is returned.
//
// If the database connection is nil, it returns ErrNilConnection.
func (r *RedshiftDatabaseClient) Ping(ctx context.Context) error {
	if r.db == nil {
		return ErrNilConnection
	}
	return r.db.PingContext(ctx)
}

// NewRedshiftDatabaseClient creates a new RedshiftDatabaseClient based on the given configuration.
// If the configuration is invalid, or if the connection can't be established,
// an error is returned. If the connection is established, but the database can't
// be pinged, the connection is closed and an error is returned.
//
// If the logger is nil, the package's default logger is used.
func NewRedshiftDatabaseClient(cfg *Config, logger *log.Logger) (*RedshiftDatabaseClient, error) {
	if logger == nil {
		logger = log.New(log.Writer(), "[REDSHIFT] ", log.LstdFlags)
	}
	if err := cfg.Validate(); err != nil {
		// If the configuration is invalid, return an error
		return nil, err
	}
	db, err := sql.Open("postgres", cfg.DNS())
	if err != nil {
		// If the connection can't be established, return an error
		return nil, err
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnTimeout)

	client := &RedshiftDatabaseClient{db: db, logger: logger}

	// verify connection
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnTimeout)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		// If the connection can't be pinged, close the connection and return an error
		client.Close()
		return nil, fmt.Errorf("failed to ping Redshift: %v", err)
	}

	return client, nil

}

// convertValueToGoType takes a value of unknown type and attempts to convert it
// to a native Go type. If the type is a []byte, it is converted to a string.
// Otherwise, the original value is returned.
func convertValueToGoType(v interface{}) interface{} {
	switch val := v.(type) {
	case []byte:
		return string(val)
	default:
		return val

	}
}
