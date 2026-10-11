package main

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func main() {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		log.Fatal("DATABASE_URL is required")
	}

	db, e := sql.Open("pgx", url)
	if e != nil {
		log.Fatal("cannot open database")
	}

	defer db.Close()
	if e = goose.SetDialect("postgres"); e != nil {
		log.Fatal(e)
	}

	if e = goose.Up(db, "migrations"); e != nil {
		log.Fatal(e)
	}
}
