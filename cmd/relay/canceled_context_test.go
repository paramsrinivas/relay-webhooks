package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCanceledHTTPContextStillRecordsFailure(t *testing.T) {
	db := isolatedTestDB(t)

	testCtx, stopTest := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer stopTest()

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			started <- struct{}{}
			<-release
		},
	))
	defer receiver.Close()
	defer close(release)

	endpointID := uuid()
	eventID := uuid()

	_, err := db.Exec(testCtx,
		`INSERT INTO endpoints(id,url,secret)
         VALUES($1,$2,$3)`,
		endpointID, receiver.URL, "cancel-test-secret",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(testCtx,
		`INSERT INTO events(id,endpoint_id,event_type,payload)
         VALUES($1,$2,$3,$4)`,
		eventID, endpointID, "test.cancel",
		[]byte(`{"cancel":true}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(testCtx,
		`INSERT INTO deliveries(event_id) VALUES($1)`,
		eventID,
	)
	if err != nil {
		t.Fatal(err)
	}

	s := &server{
		db:     db,
		client: receiver.Client(),
	}

	job, err := s.claim(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("expected a claimed delivery")
	}

	deliveryCtx, cancelDelivery := context.WithCancel(
		context.Background(),
	)
	defer cancelDelivery()

	done := make(chan struct{})

	go func() {
		s.deliver(deliveryCtx, job)
		close(done)
	}()

	// Wait until the receiver has accepted the request.
	select {
	case <-started:
	case <-testCtx.Done():
		t.Fatal("receiver never received webhook")
	}

	// Cancel the HTTP request while it is in progress.
	cancelDelivery()

	select {
	case <-done:
	case <-testCtx.Done():
		t.Fatal("delivery did not finish after cancellation")
	}

	var state string
	var attempts int
	var nextAttempt time.Time
	var lastError *string
	var leaseUntil *time.Time
	var leaseToken *string

	err = db.QueryRow(testCtx,
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

	if lastError == nil ||
		!strings.Contains(strings.ToLower(*lastError), "cancel") {
		t.Fatalf("expected cancellation error, got %v", lastError)
	}

	if !nextAttempt.After(time.Now()) {
		t.Fatal("retry was not scheduled")
	}

	if leaseUntil != nil || leaseToken != nil {
		t.Fatal("lease was not released after cancellation")
	}

	t.Log("canceled HTTP context did not prevent DB finalization")
}
