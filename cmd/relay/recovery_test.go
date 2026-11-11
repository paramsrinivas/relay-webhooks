package main

import (
	"context"
	"testing"
	"time"
)

func TestExpiredLeaseRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := isolatedTestDB(t)
	var err error

	endpointID := uuid()
	eventID := uuid()

	_, err = db.Exec(ctx,
		`INSERT INTO endpoints(id,url,secret)
 VALUES($1,$2,$3)`,
		endpointID, "https://example.com/webhook", "test-secret",
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		c, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()

		if _, err := db.Exec(c,
			`DELETE FROM deliveries WHERE event_id=$1`, eventID); err != nil {
			t.Errorf("cleanup delivery: %v", err)
		}
		if _, err := db.Exec(c,
			`DELETE FROM events WHERE id=$1`, eventID); err != nil {
			t.Errorf("cleanup event: %v", err)
		}
		if _, err := db.Exec(c,
			`DELETE FROM endpoints WHERE id=$1`, endpointID); err != nil {
			t.Errorf("cleanup endpoint: %v", err)
		}
	})

	_, err = db.Exec(ctx,
		`INSERT INTO events(id,endpoint_id,event_type,payload)
 VALUES($1,$2,$3,$4)`,
		eventID, endpointID, "test.crash", `{"test":true}`,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx,
		`INSERT INTO deliveries(event_id) VALUES($1)`, eventID)
	if err != nil {
		t.Fatal(err)
	}

	s := &server{db: db}

	// Worker A claims the delivery.
	first, err := s.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || first.EventID != eventID {
		t.Fatalf("worker A did not claim expected event")
	}

	// A valid lease must prevent another claim.
	second, err := s.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second != nil {
		t.Fatalf("delivery claimed while lease was still valid: %s", second.EventID)
	}

	// Simulate Worker A crashing and its lease expiring.
	_, err = db.Exec(ctx,
		`UPDATE deliveries
 SET lease_until=now()-interval '1 second'
 WHERE event_id=$1`, eventID)
	if err != nil {
		t.Fatal(err)
	}

	// Worker B should reclaim the expired delivery.
	recovered, err := s.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovered == nil || recovered.EventID != eventID {
		t.Fatalf("worker B did not recover expected event")
	}

	if recovered.Token == first.Token {
		t.Error("recovered lease reused old token")
	}

	if recovered.Attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", recovered.Attempts)
	}

	var storedToken string
	var attempts int
	var state string

	err = db.QueryRow(ctx,
		`SELECT lease_token::text,attempts,state
 FROM deliveries WHERE event_id=$1`,
		eventID,
	).Scan(&storedToken, &attempts, &state)
	if err != nil {
		t.Fatal(err)
	}

	if storedToken != recovered.Token {
		t.Error("database does not contain new lease token")
	}
	if attempts != 2 {
		t.Errorf("database attempts: expected 2, got %d", attempts)
	}
	if state != "leased" {
		t.Errorf("expected leased state, got %s", state)
	}

	t.Logf("Recovered expired lease: attempts=%d, token rotated", attempts)
}

func TestStaleWorkerCannotOverwriteNewLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := isolatedTestDB(t)
	var err error

	endpointID := uuid()
	eventID := uuid()
	oldToken := uuid()
	newToken := uuid()

	_, err = db.Exec(ctx,
		`INSERT INTO endpoints(id,url,secret) VALUES($1,$2,$3)`,
		endpointID, "https://example.com/webhook", "test-secret",
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		c, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		db.Exec(c, `DELETE FROM deliveries WHERE event_id=$1`, eventID)
		db.Exec(c, `DELETE FROM events WHERE id=$1`, eventID)
		db.Exec(c, `DELETE FROM endpoints WHERE id=$1`, endpointID)
	})

	_, err = db.Exec(ctx,
		`INSERT INTO events(id,endpoint_id,event_type,payload)
 VALUES($1,$2,$3,$4)`,
		eventID, endpointID, "test.stale", `{"test":true}`,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx,
		`INSERT INTO deliveries(event_id,state,attempts,lease_token,lease_until)
 VALUES($1,'leased',2,$2,now()+interval '30 seconds')`,
		eventID, newToken,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Old worker tries to mark the delivery successful.
	tag, err := db.Exec(ctx,
		`UPDATE deliveries
 SET state='delivered',delivered_at=now(),
     lease_until=NULL,lease_token=NULL,last_error=NULL
 WHERE event_id=$1 AND lease_token=$2 AND state='leased'`,
		eventID, oldToken,
	)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 0 {
		t.Fatalf("stale worker modified %d rows", tag.RowsAffected())
	}

	// Old worker tries to record a failure.
	tag, err = db.Exec(ctx,
		`UPDATE deliveries
 SET state='pending',next_attempt_at=now(),
     lease_until=NULL,lease_token=NULL,last_error='stale failure'
 WHERE event_id=$1 AND lease_token=$2 AND state='leased'`,
		eventID, oldToken,
	)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 0 {
		t.Fatalf("stale worker failure modified %d rows", tag.RowsAffected())
	}

	var state, storedToken string
	err = db.QueryRow(ctx,
		`SELECT state,lease_token::text FROM deliveries WHERE event_id=$1`,
		eventID,
	).Scan(&state, &storedToken)
	if err != nil {
		t.Fatal(err)
	}

	if state != "leased" {
		t.Errorf("expected leased state, got %s", state)
	}
	if storedToken != newToken {
		t.Error("new worker's lease token was overwritten")
	}

	t.Log("Stale success and failure updates rejected; new lease preserved")
}
