package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq" // Import sterownika PostgreSQL
)

type RedshiftHandler struct {
	db *sql.DB
}

// NewRedshiftHandler creates a new RedshiftHandler based on the connection string
// passed in. The connection string should be in the format accepted by the
// lib/pq driver, e.g. "user:password@localhost:5432/database?sslmode=disable".
// If the connection string is invalid, or if the connection can't be established,
// an error is returned. If the connection is established, but the database can't
// be pinged, the connection is closed and an error is returned.
func NewRedshiftHandler(connStr string) (*RedshiftHandler, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to Redshift: %v", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping Redshift: %v", err)
		db.Close()
		return nil, fmt.Errorf("Failed to ping Redshift: %v", err)
	}

	return &RedshiftHandler{db: db}, nil

}

// Query executes the given query on the Redshift database with the given arguments
// and returns the result as a *sql.Rows. sql.Rows its cursor starts before the first row
// If the query fails, an error is returned.
func (h *RedshiftHandler) Query(query string, args ...interface{}) (*sql.Rows, error) {
	log.Printf("Executing query: %s with args: %v", query, args)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("Failed to execute query: %v", err)
	}
	return rows, nil

}

// Close closes the connection to the Redshift database.
func (r *RedshiftHandler) Close() {
	r.db.Close()
}

// Ping checks if the connection to the Redshift database is still alive,
// establishing a connection if necessary.
func (r *RedshiftHandler) Ping() error {
	return r.db.Ping()
}

func (r *RedshiftHandler) FetchChunks(query string, chunkSize int, args ...interface{}) ([][]map[string]interface{}, error) {

	log.Printf("Executing query: %s with args: %v", query, args)
	rows, err := r.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	results := [][]map[string]interface{}{}
	chunk := []map[string]interface{}{}

	for rows.Next() {
		// create slice for values
		values := make([]interface{}, len(columns))
		valuesPointers := make([]interface{}, len(columns))
		for i := range values {
			valuesPointers[i] = &values[i]
		}
		// scan values into slice
		if err := rows.Scan(valuesPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// create map for the rows
		row := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			switch val.(type) {
			case []byte:
				v = string(val.([]byte)) // convert []byte to string
			default:
				v = val
			}
			row[col] = v
		}
		chunk = append(chunk, row)
		// Check if chunk is full
		if len(chunk) == chunkSize {
			results = append(results, chunk)
			chunk = []map[string]interface{}{}
		}
	}
	// Check if there are ramaining rows to add
	if len(chunk) > 0 {
		results = append(results, chunk)
	}
	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate rows: %v", err)
	}

	return results, nil
}

func main() {

	connStr := "postgres://user:password@localhost:5439/dbname?sslmode=disable"
	// Create a new RedshiftHandler

	handler, err := NewRedshiftHandler(connStr)
	if err != nil {
		log.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

}
