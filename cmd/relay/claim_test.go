package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestConcurrentClaims(t *testing.T) {
	const workers = 20

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	db := isolatedTestDB(t)
	var err error

	// Create a unique endpoint for this test.
	endpointID := uuid()

	_, err = db.Exec(ctx,
		`INSERT INTO endpoints(id, url, secret)
 VALUES($1, $2, $3)`,
		endpointID,
		"https://example.com/webhook",
		"test-secret",
	)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}

	// Clean up even if the test fails.
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer done()

		_, err := db.Exec(cleanupCtx,
			`DELETE FROM deliveries
 WHERE event_id IN (
     SELECT id FROM events WHERE endpoint_id=$1
 )`,
			endpointID,
		)
		if err != nil {
			t.Errorf("cleanup deliveries: %v", err)
		}

		_, err = db.Exec(cleanupCtx,
			`DELETE FROM events WHERE endpoint_id=$1`,
			endpointID,
		)
		if err != nil {
			t.Errorf("cleanup events: %v", err)
		}

		_, err = db.Exec(cleanupCtx,
			`DELETE FROM endpoints WHERE id=$1`,
			endpointID,
		)
		if err != nil {
			t.Errorf("cleanup endpoint: %v", err)
		}
	})

	// Create 20 independent delivery jobs.
	expected := make(map[string]bool)

	for i := 0; i < workers; i++ {
		eventID := uuid()
		expected[eventID] = true

		_, err = db.Exec(ctx,
			`INSERT INTO events(
    id, endpoint_id, event_type, payload
 ) VALUES($1, $2, $3, $4)`,
			eventID,
			endpointID,
			"test.concurrent",
			fmt.Sprintf(`{"number":%d}`, i),
		)
		if err != nil {
			t.Fatalf("create event %d: %v", i, err)
		}

		_, err = db.Exec(ctx,
			`INSERT INTO deliveries(event_id)
 VALUES($1)`,
			eventID,
		)
		if err != nil {
			t.Fatalf("create delivery %d: %v", i, err)
		}
	}

	s := &server{db: db}

	type result struct {
		job *job
		err error
	}

	results := make(chan result, workers)
	start := make(chan struct{})

	var wg sync.WaitGroup

	// Start 20 workers simultaneously.
	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			j, err := s.claim(ctx)

			results <- result{
				job: j,
				err: err,
			}
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	seenEvents := make(map[string]bool)
	seenTokens := make(map[string]bool)
	eventTokens := make(map[string]string)

	// Verify every returned claim.
	for r := range results {
		if r.err != nil {
			t.Errorf("claim failed: %v", r.err)
			continue
		}

		if r.job == nil {
			t.Error("worker received no job")
			continue
		}

		if !expected[r.job.EventID] {
			t.Errorf(
				"unexpected event claimed: %s",
				r.job.EventID,
			)
			continue
		}

		if seenEvents[r.job.EventID] {
			t.Errorf(
				"DUPLICATE CLAIM: %s",
				r.job.EventID,
			)
		}

		if seenTokens[r.job.Token] {
			t.Errorf(
				"DUPLICATE LEASE TOKEN: %s",
				r.job.Token,
			)
		}

		seenEvents[r.job.EventID] = true
		seenTokens[r.job.Token] = true
		eventTokens[r.job.EventID] = r.job.Token

		if r.job.Attempts != 1 {
			t.Errorf(
				"event %s: expected 1 attempt, got %d",
				r.job.EventID,
				r.job.Attempts,
			)
		}
	}

	// Independently verify PostgreSQL lease state.
	for eventID, expectedToken := range eventTokens {
		var state string
		var attempts int
		var storedToken string
		var leaseUntil time.Time

		err := db.QueryRow(ctx,
			`SELECT state, attempts, lease_token, lease_until
 FROM deliveries
 WHERE event_id=$1`,
			eventID,
		).Scan(
			&state,
			&attempts,
			&storedToken,
			&leaseUntil,
		)

		if err != nil {
			t.Errorf("read delivery %s: %v", eventID, err)
			continue
		}

		if state != "leased" {
			t.Errorf(
				"event %s: expected leased, got %s",
				eventID, state,
			)
		}

		if attempts != 1 {
			t.Errorf(
				"event %s: expected 1 attempt, got %d",
				eventID, attempts,
			)
		}

		if storedToken != expectedToken {
			t.Errorf(
				"event %s: lease token mismatch",
				eventID,
			)
		}

		if !leaseUntil.After(time.Now()) {
			t.Errorf(
				"event %s: lease already expired",
				eventID,
			)
		}
	}

	if len(seenEvents) != workers {
		t.Errorf(
			"expected %d unique claims, got %d",
			workers,
			len(seenEvents),
		)
	}

	t.Logf(
		"Unique deliveries claimed: %d/%d",
		len(seenEvents),
		workers,
	)
}
