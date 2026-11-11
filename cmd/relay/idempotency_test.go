package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentIdempotency(t *testing.T) {
	const workers = 20

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := "postgres://relay:relay@localhost:5432/relay_test?sslmode=disable"

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("connect to test database: %v", err)
	}

	endpointID := uuid()
	_, err = db.Exec(ctx,
		`INSERT INTO endpoints(id,url,secret) VALUES($1,$2,$3)`,
		endpointID, "https://example.com/webhook", "test-secret",
	)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}

	t.Cleanup(func() {
		c, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()

		if _, err := db.Exec(c,
			`DELETE FROM deliveries WHERE event_id IN
 (SELECT id FROM events WHERE endpoint_id=$1)`,
			endpointID); err != nil {
			t.Errorf("cleanup deliveries: %v", err)
		}
		if _, err := db.Exec(c,
			`DELETE FROM events WHERE endpoint_id=$1`,
			endpointID); err != nil {
			t.Errorf("cleanup events: %v", err)
		}
		if _, err := db.Exec(c,
			`DELETE FROM endpoints WHERE id=$1`,
			endpointID); err != nil {
			t.Errorf("cleanup endpoint: %v", err)
		}
	})

	s := &server{db: db}

	body, err := json.Marshal(map[string]any{
		"endpoint_id": endpointID,
		"type":        "payment.created",
		"payload":     map[string]any{"amount": 100},
	})
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		code int
		id   string
		err  error
	}

	results := make(chan result, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			req := httptest.NewRequest(
				http.MethodPost, "/events", bytes.NewReader(body),
			)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "same-key-123")

			rec := httptest.NewRecorder()
			s.events(rec, req)

			var response struct {
				EventID string `json:"event_id"`
			}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			results <- result{
				code: rec.Code,
				id:   response.EventID,
				err:  err,
			}
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	ids := make(map[string]bool)

	for r := range results {
		if r.err != nil {
			t.Errorf("decode response: %v", r.err)
			continue
		}
		if r.code != http.StatusAccepted {
			t.Errorf("expected HTTP 202, got %d", r.code)
		}
		if r.id == "" {
			t.Error("missing event ID")
			continue
		}
		ids[r.id] = true
	}

	if len(ids) != 1 {
		t.Errorf("expected one unique event ID, got %d", len(ids))
	}

	var eventCount int
	err = db.QueryRow(ctx,
		`SELECT COUNT(*) FROM events
 WHERE endpoint_id=$1 AND idempotency_key=$2`,
		endpointID, "same-key-123",
	).Scan(&eventCount)
	if err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Errorf("expected 1 event, got %d", eventCount)
	}

	var deliveryCount int
	err = db.QueryRow(ctx,
		`SELECT COUNT(*) FROM deliveries d
 JOIN events e ON e.id=d.event_id
 WHERE e.endpoint_id=$1`,
		endpointID,
	).Scan(&deliveryCount)
	if err != nil {
		t.Fatal(err)
	}
	if deliveryCount != 1 {
		t.Errorf("expected 1 delivery, got %d", deliveryCount)
	}

	t.Log(fmt.Sprintf(
		"Requests: %d, unique IDs: %d, events: %d, deliveries: %d",
		workers, len(ids), eventCount, deliveryCount,
	))
}

func TestIdempotencyKeyPayloadConflict(t *testing.T) {
	ctx := context.Background()
	dsn := "postgres://relay:relay@localhost:5432/relay_test?sslmode=disable"

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	endpointID := uuid()
	_, err = db.Exec(ctx,
		`INSERT INTO endpoints(id,url,secret) VALUES($1,$2,$3)`,
		endpointID, "https://example.com/webhook", "test-secret",
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		db.Exec(c, `DELETE FROM deliveries WHERE event_id IN
(SELECT id FROM events WHERE endpoint_id=$1)`, endpointID)
		db.Exec(c, `DELETE FROM events WHERE endpoint_id=$1`, endpointID)
		db.Exec(c, `DELETE FROM endpoints WHERE id=$1`, endpointID)
	})

	s := &server{db: db}

	send := func(amount int) int {
		body := fmt.Sprintf(
			`{"endpoint_id":"%s","type":"payment.created","payload":{"amount":%d}}`,
			endpointID, amount,
		)
		req := httptest.NewRequest(http.MethodPost, "/events",
			bytes.NewBufferString(body))
		req.Header.Set("Idempotency-Key", "conflict-key-123")
		rec := httptest.NewRecorder()
		s.events(rec, req)
		return rec.Code
	}

	if code := send(100); code != http.StatusAccepted {
		t.Fatalf("first request: expected 202, got %d", code)
	}

	if code := send(500); code != http.StatusConflict {
		t.Errorf("second request: expected 409 Conflict, got %d", code)
	}
}
