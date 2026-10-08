package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	tianasqlite "github.com/tianacloud/sdk-go-sqlite"
)

func main() {
	gateway, err := gatewayConfig()
	if err != nil {
		log.Fatal(err)
	}
	connector, err := tianasqlite.NewConnector(tianasqlite.Config{Gateway: gateway})
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(30 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var value string
	if err = db.QueryRowContext(ctx, "SELECT ?", "Hello, Tiana!").Scan(&value); err != nil {
		log.Fatal(err)
	}
	log.Print(value)
}
