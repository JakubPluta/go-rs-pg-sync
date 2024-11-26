package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/JakubPluta/go-rs-pg-sync/internals/db"
)

func main() {
	// TODO: implement

	cfg := &db.Config{
		Host:         "localhost",
		Port:         5439,
		User:         "username",
		Password:     "password",
		Database:     "database",
		SSLMode:      "disable",
		MaxOpenConns: 10,
		MaxIdleConns: 5,
		ConnTimeout:  5 * time.Second,
	}

	client, err := db.NewRedshiftDatabaseClient(cfg, nil)
	if err != nil {
		log.Fatal(err)
		// handle error
		return
	}
	defer client.Close()

	rows, err := client.FetchChunks(context.Background(), "SELECT * FROM table.table limit", 100)
	if err != nil {
		log.Fatal(err)
		// handle error
		return
	}

	for _, chunk := range rows {
		for idx, row := range chunk {
			fmt.Println(idx, row)
		}
	}

}
