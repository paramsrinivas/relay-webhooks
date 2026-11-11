package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWebhookTimeoutSchedulesRetry(t *testing.T) {
	db := isolatedTestDB(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	started := make(chan struct{})
	release := make(chan struct{})

	// The receiver accepts the request but blocks
	// without sending an HTTP response.
	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-release
			w.WriteHeader(http.StatusOK)
		},
	))
	defer receiver.Close()
	defer close(release)

	endpointID := uuid()
	eventID := uuid()

	_, err := db.Exec(ctx,
		`INSERT INTO endpoints(id,url,secret)
 VALUES($1,$2,$3)`,
		endpointID, receiver.URL, "timeout-secret",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx,
		`INSERT INTO events(id,endpoint_id,event_type,payload)
 VALUES($1,$2,$3,$4)`,
		eventID, endpointID, "test.timeout",
		[]byte(`{"test":true}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx,
		`INSERT INTO deliveries(event_id) VALUES($1)`,
		eventID,
	)
	if err != nil {
		t.Fatal(err)
	}

	client := receiver.Client()
	client.Timeout = 150 * time.Millisecond

	s := &server{
		db:     db,
		client: client,
	}

	job, err := s.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("expected delivery to be claimed")
	}

	s.deliver(ctx, job)

	select {
	case <-started:
	default:
		t.Fatal("receiver never received the request")
	}

	var state string
	var attempts int
	var nextAttempt time.Time
	var lastError *string
	var leaseUntil *time.Time
	var leaseToken *string

	err = db.QueryRow(ctx,
		`SELECT state, attempts, next_attempt_at,
        last_error, lease_until, lease_token::text
 FROM deliveries WHERE event_id=$1`,
		eventID,
	).Scan(
		&state, &attempts, &nextAttempt,
		&lastError, &leaseUntil, &leaseToken,
	)
	if err != nil {
		t.Fatal(err)
	}

	if state != "pending" {
		t.Fatalf("expected pending, got %s", state)
	}

	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}

	if !nextAttempt.After(time.Now()) {
		t.Fatal("retry was not scheduled in the future")
	}

	if lastError == nil {
		t.Fatal("timeout error was not recorded")
	}

	message := strings.ToLower(*lastError)
	if !strings.Contains(message, "timeout") &&
		!strings.Contains(message, "deadline exceeded") {
		t.Fatalf("expected timeout error, got %q", *lastError)
	}

	if leaseUntil != nil || leaseToken != nil {
		t.Fatal("delivery lease was not released")
	}

	t.Log("HTTP timeout recorded and retry scheduled")
}
