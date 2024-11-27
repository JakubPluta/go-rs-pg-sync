package main

import (
	"log"

	"github.com/JakubPluta/go-rs-pg-sync/internals/db"
)

func main() {
	// PostgreSQL
	pgConfig := db.NewDefaultConfig(db.TypePostgres)
	pgConfig.Host = "localhost"
	pgConfig.Database = "mydb"
	pgConfig.User = "user"
	pgConfig.Password = "pass"

	pgClient, err := db.NewClient(pgConfig, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer pgClient.Close()

	// Redshift
	rsConfig := db.NewDefaultConfig(db.TypeRedshift)
	rsConfig.Host = "redshift-cluster.amazonaws.com"
	rsConfig.Database = "warehouse"
	rsConfig.User = "admin"
	rsConfig.Password = "pass"

	rsClient, err := db.NewClient(rsConfig, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer rsClient.Close()
}
