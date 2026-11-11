package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func isolatedTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const dsn = "postgres://relay:relay@localhost:5432/relay_test?sslmode=disable"

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}

	schema := "test_" + fmt.Sprintf("%d", time.Now().UnixNano()) + "_" + uuid()[:8]

	_, err = admin.Exec(ctx, `CREATE SCHEMA "`+schema+`"`)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}

	config.ConnConfig.RuntimeParams["search_path"] = schema

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		db.Close()

		c, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()

		_, err := admin.Exec(c, `DROP SCHEMA "`+schema+`" CASCADE`)
		if err != nil {
			t.Errorf("drop test schema: %v", err)
		}

		admin.Close()
	})

	migration, err := os.ReadFile("../../migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx, string(migration))
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}
