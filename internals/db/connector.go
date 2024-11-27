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

// DatabaseType represents the type of database to connect to.
type DatabaseType string

// Default Configuration Values
const (
	// Supported Database Types
	TypePostgres DatabaseType = "postgres"
	TypeRedshift DatabaseType = "redshift"

	// Default Configuration Values
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
	Type         DatabaseType
	Host         string
	Port         int
	User         string
	Password     string
	Database     string
	SSLMode      string
	MaxOpenConns int
	MaxIdleConns int
	ConnTimeout  time.Duration // // Connection timeout duration

	// Optional, Additional database-specific options can be added here
	Options map[string]string
}

// NewDefaultConfig creates a new Config instance with default values.
// These values can be overridden before passing to the NewClient function.
//
// Default values:
// - Port: 5439
// - SSLMode: disable
// - MaxOpenConns: 10
// - MaxIdleConns: 5
// - ConnTimeout: 30 seconds
func NewDefaultConfig(dbType DatabaseType) *Config {
	config := &Config{
		Type:         dbType,
		Port:         defaultPort,
		SSLMode:      defaultSSLMode,
		MaxOpenConns: defaultMaxOpenConns,
		MaxIdleConns: defaultMaxIdleConns,
		ConnTimeout:  defaultConnTimeout,
		Options:      make(map[string]string),
	}

	switch dbType {
	case TypeRedshift:
		config.Port = 5439 // Default port for Redshift
	default:
		config.Port = 5432 // Default port for PostgreSQL

	}
	return config
}

// DNS generates a connection string in DNS format (i.e. postgres://user:pass@host:port/dbname?sslmode=mode)
// from the given configuration.
func (c *Config) DNS() string {
	baseUrl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode)
	// Add additional options
	if len(c.Options) > 0 {
		for key, value := range c.Options {
			baseUrl += fmt.Sprintf("&%s=%s", key, value)
		}
	}
	return baseUrl

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
// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	switch {
	case c.Type == "":
		return fmt.Errorf("%v: database type must be specified", ErrInvalidConfig)
	case c.Host == "":
		return fmt.Errorf("%v: host cannot be empty", ErrInvalidConfig)
	case c.Port <= 0:
		return fmt.Errorf("%v: invalid port number", ErrInvalidConfig)
	case c.User == "":
		return fmt.Errorf("%v: user cannot be empty", ErrInvalidConfig)
	case c.Database == "":
		return fmt.Errorf("%v: database cannot be empty", ErrInvalidConfig)
	case c.MaxOpenConns < c.MaxIdleConns:
		return fmt.Errorf("%v: maxOpenConns must be >= maxIdleConns", ErrInvalidConfig)
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
	FetchChunks(ctx context.Context, query string, chunkSize int, args ...interface{}) ([][]Row, error)

	// Close closes the connection to the database.
	Close() error

	// Ping checks if the connection to the database is still alive,
	// establishing a connection if necessary. If the connection is alive,
	// the method returns nil. Otherwise, an error is returned.
	Ping(ctx context.Context) error

	// Type returns the type of the database client
	Type() DatabaseType
}

// Client implements the DatabaseClient interface
type Client struct {
	db         *sql.DB
	logger     *log.Logger
	dbType     DatabaseType
	statements map[string]*sql.Stmt // Map to store prepared statements
}

func (c *Client) Type() DatabaseType {
	return c.dbType
}

func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if c.db == nil {
		// Return an error if the database connection is nil. It can happen if the database client is closed
		return nil, ErrNilConnection
	}

	c.logger.Printf("executing query: %s with args: %v", query, args)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		// Return an error if the query fails
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

// FetchChunks executes a query on the database with the given arguments and
// returns the result as a slice of slices of maps. Each inner slice represents a chunk of data and each map represents a row in the chunk.
// The size of each chunk is determined by the chunkSize parameter. If the query fails, an error is returned.
func (c *Client) FetchChunks(ctx context.Context, query string, chunkSize int, args ...interface{}) ([][]Row, error) {
	if chunkSize <= 0 {
		return nil, ErrInvalidChunkSize
	}
	rows, err := c.Query(ctx, query, args...)
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
			row[col] = convertValueType(values[i]) // convert value to go type
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

func (c *Client) Close() error {
	if c.db == nil {
		return ErrNilConnection
	}
	// Close all prepared statements. It's important to do this before closing the connection
	for _, stmt := range c.statements {
		stmt.Close()
	}

	return c.db.Close()
}

// Ping checks if the connection to the database is still alive,
// establishing a connection if necessary. If the connection is alive,
// the method returns nil. Otherwise, an error is returned.
//
// If the database connection is nil, it returns ErrNilConnection.
func (c *Client) Ping(ctx context.Context) error {
	if c.db == nil {
		return ErrNilConnection
	}
	return c.db.PingContext(ctx)
}

// NewClient creates a new instance of the Client type based on the given configuration.
// It creates a connection to the database and verifies it by pinging it.
// If the configuration is invalid, the connection can't be established or the ping fails,
// it returns an error. Otherwise, it returns a new instance of the Client type.
func NewClient(cfg *Config, logger *log.Logger) (*Client, error) {
	if logger == nil {
		prefix := fmt.Sprintf("[%s] ", cfg.Type)
		logger = log.New(log.Writer(), prefix, log.LstdFlags)
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

	client := &Client{db: db, logger: logger, dbType: cfg.Type, statements: make(map[string]*sql.Stmt)}

	// verify connection
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnTimeout)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		// If the connection can't be pinged, close the connection and return an error
		client.Close()
		return nil, fmt.Errorf("failed to ping client: %v", err)
	}

	return client, nil

}

// convertValueToGoType takes a value of unknown type and attempts to convert it
// to a native Go type. If the type is a []byte, it is converted to a string.
// Otherwise, the original value is returned.
func convertValueType(v interface{}) interface{} {
	switch val := v.(type) {
	case []byte:
		return string(val)
	default:
		return val

	}
}
