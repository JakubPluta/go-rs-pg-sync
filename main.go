package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/JakubPluta/go-rs-pg-sync/internals/db"
	"github.com/JakubPluta/go-rs-pg-sync/internals/sync"
)

func main() {
	logger := log.New(os.Stdout, "[SYNC] ", log.LstdFlags|log.Lmicroseconds)

	sourceConfig := db.NewDefaultConfig(db.TypeRedshift)
	sourceConfig.Host = os.Getenv("REDSHIFT_HOST")
	sourceConfig.Port = 5439
	sourceConfig.Database = os.Getenv("REDSHIFT_DB")
	sourceConfig.User = os.Getenv("REDSHIFT_USER")
	sourceConfig.Password = os.Getenv("REDSHIFT_PASSWORD")
	sourceConfig.MaxOpenConns = 10
	sourceConfig.MaxIdleConns = 5
	sourceConfig.ConnTimeout = 30 * time.Second

	targetConfig := db.NewDefaultConfig(db.TypePostgres)
	targetConfig.Host = os.Getenv("PG_HOST")
	targetConfig.Port = 5432
	targetConfig.Database = os.Getenv("PG_DB")
	targetConfig.User = os.Getenv("PG_USER")
	targetConfig.Password = os.Getenv("PG_PASSWORD")
	targetConfig.MaxOpenConns = 20
	targetConfig.MaxIdleConns = 10
	targetConfig.ConnTimeout = 30 * time.Second

	sourceDB, err := db.NewClient(sourceConfig, logger)
	if err != nil {
		logger.Fatalf("Failed to create source client: %v", err)
	}
	defer sourceDB.Close()

	targetDB, err := db.NewClient(targetConfig, logger)
	if err != nil {
		logger.Fatalf("Failed to create target client: %v", err)
	}
	defer targetDB.Close()

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

	// Konfiguracja synchronizacji
	syncConfig := &sync.SyncConfig{
		Source:           sourceConfig,
		Target:           targetConfig,
		Direction:        sync.RedshiftToPostgres,
		DefaultBatchSize: 1000,
		MaxConcurrency:   5,
		RetryAttempts:    3,
		RetryDelay:       5, // sekundy
		Tables:           tablesMappings,
	}

	// Create syncer
	syncer, err := sync.NewDatabaseSyncer(sourceDB, targetDB, syncConfig, logger)
	if err != nil {
		logger.Fatalf("Failed to create syncer: %v", err)
	}

	// Context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	// Start synchronization task
	logger.Println("Starting database synchronization...")
	startTime := time.Now()

	if err := syncer.Sync(ctx); err != nil {
		logger.Fatalf("Synchronization failed: %v", err)
	}

	logger.Printf("Synchronization completed successfully in %v", time.Since(startTime))
}
