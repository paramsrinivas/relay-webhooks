package main

import (
    "context"
    "testing"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
)

func TestDatabaseConnection(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    dsn := "postgres://relay:relay@localhost:5432/relay?sslmode=disable"

    db, err := pgxpool.New(ctx, dsn)
    if err != nil {
        t.Fatalf("create database pool: %v", err)
    }
    defer db.Close()

    if err := db.Ping(ctx); err != nil {
        t.Fatalf("connect to PostgreSQL: %v", err)
    }

    t.Log("PostgreSQL connection successful")
}
