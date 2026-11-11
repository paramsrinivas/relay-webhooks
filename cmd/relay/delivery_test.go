package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSuccessfulWebhookDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	db := isolatedTestDB(t)

	secret := "integration-test-secret"
	payload := []byte(`{"order_id":"123"}`)
	eventID := uuid()
	endpointID := uuid()

	received := make(chan *http.Request, 1)
	bodies := make(chan []byte, 1)
	receivedSignature := make(chan *http.Request, 1)

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			bodies <- body
			received <- r
			receivedSignature <- r
			w.WriteHeader(http.StatusOK)
		},
	))
	defer receiver.Close()

	_, err := db.Exec(ctx,
		`INSERT INTO endpoints(id,url,secret)
 VALUES($1,$2,$3)`,
		endpointID, receiver.URL, secret,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx,
		`INSERT INTO events(id,endpoint_id,event_type,payload)
 VALUES($1,$2,$3,$4)`,
		eventID, endpointID, "order.created", payload,
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

	s := &server{
		db:     db,
		client: receiver.Client(),
	}

	j, err := s.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if j == nil || j.EventID != eventID {
		t.Fatal("did not claim expected event")
	}

	s.deliver(ctx, j)

	select {
	case req := <-received:
		if req.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", req.Method)
		}
		if req.Header.Get("X-Relay-Event-ID") != eventID {
			t.Error("incorrect event ID header")
		}
		if req.Header.Get("X-Relay-Event-Type") != "order.created" {
			t.Error("incorrect event type header")
		}
	case <-ctx.Done():
		t.Fatal("receiver did not get webhook")
	}

	select {
	case body := <-bodies:
		req := <-receivedSignature
		header := req.Header.Get("X-Relay-Signature")
		parts := strings.Split(header, ",v1=")
		if len(parts) != 2 || !strings.HasPrefix(parts[0], "t=") {
			t.Fatalf("invalid signature header: %q", header)
		}

		timestamp := strings.TrimPrefix(parts[0], "t=")
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(timestamp + "."))
		mac.Write(body)
		expectedMAC := mac.Sum(nil)

		actualMAC, err := hex.DecodeString(parts[1])
		if err != nil {
			t.Fatalf("invalid signature encoding: %v", err)
		}
		if !hmac.Equal(actualMAC, expectedMAC) {
			t.Fatal("webhook HMAC signature verification failed")
		}
		if !json.Valid(body) || !json.Valid(payload) {
			t.Fatal("invalid JSON payload")
		}
		var actual, expected any
		if err := json.Unmarshal(body, &actual); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(payload, &expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("unexpected payload: %s", body)
		}
	default:
		t.Fatal("receiver body missing")
	}

	var state string
	var attempts int
	var deliveredAt *time.Time
	var leaseToken *string

	err = db.QueryRow(ctx,
		`SELECT state,attempts,delivered_at,lease_token::text
 FROM deliveries WHERE event_id=$1`,
		eventID,
	).Scan(&state, &attempts, &deliveredAt, &leaseToken)
	if err != nil {
		t.Fatal(err)
	}

	if state != "delivered" {
		t.Errorf("expected delivered, got %s", state)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
	if deliveredAt == nil {
		t.Error("delivered_at was not set")
	}
	if leaseToken != nil {
		t.Error("lease token was not cleared")
	}

	t.Log("Webhook received HTTP 200 and delivery marked delivered")
}

func TestHTTP500SchedulesRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	db := isolatedTestDB(t)

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		},
	))
	defer receiver.Close()

	endpointID := uuid()
	eventID := uuid()

	_, err := db.Exec(ctx,
		`INSERT INTO endpoints(id,url,secret)
 VALUES($1,$2,$3)`,
		endpointID, receiver.URL, "test-secret",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx,
		`INSERT INTO events(id,endpoint_id,event_type,payload)
 VALUES($1,$2,$3,$4)`,
		eventID, endpointID, "test.failed",
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

	s := &server{
		db:     db,
		client: receiver.Client(),
	}

	j, err := s.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if j == nil || j.EventID != eventID {
		t.Fatal("failed to claim expected event")
	}

	s.deliver(ctx, j)

	var state, lastError string
	var attempts int
	var nextAttempt time.Time
	var leaseToken *string

	err = db.QueryRow(ctx,
		`SELECT state, attempts, next_attempt_at,
        last_error, lease_token::text
 FROM deliveries WHERE event_id=$1`,
		eventID,
	).Scan(
		&state, &attempts, &nextAttempt,
		&lastError, &leaseToken,
	)
	if err != nil {
		t.Fatal(err)
	}

	if state != "pending" {
		t.Errorf("expected pending, got %s", state)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
	if lastError != "HTTP 500" {
		t.Errorf("expected HTTP 500, got %q", lastError)
	}
	if !nextAttempt.After(time.Now()) {
		t.Error("retry was not scheduled in the future")
	}
	if leaseToken != nil {
		t.Error("lease token was not cleared")
	}

	t.Logf(
		"HTTP 500 handled: state=%s, attempts=%d, retry=%s",
		state, attempts, nextAttempt.Format(time.RFC3339),
	)
}

func TestWebhookRetryEventuallySucceeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	db := isolatedTestDB(t)

	var requests atomic.Int32

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if requests.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		},
	))
	defer receiver.Close()

	endpointID := uuid()
	eventID := uuid()

	_, err := db.Exec(ctx,
		`INSERT INTO endpoints(id,url,secret)
 VALUES($1,$2,$3)`,
		endpointID, receiver.URL, "retry-secret",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx,
		`INSERT INTO events(id,endpoint_id,event_type,payload)
 VALUES($1,$2,$3,$4)`,
		eventID, endpointID, "retry.test",
		[]byte(`{"retry":true}`),
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

	s := &server{
		db:     db,
		client: receiver.Client(),
	}

	first, err := s.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || first.EventID != eventID {
		t.Fatal("first claim failed")
	}

	s.deliver(ctx, first)

	var state string
	err = db.QueryRow(ctx,
		`SELECT state FROM deliveries WHERE event_id=$1`,
		eventID,
	).Scan(&state)
	if err != nil {
		t.Fatal(err)
	}
	if state != "pending" {
		t.Fatalf("expected pending after HTTP 500, got %s", state)
	}

	// Simulate the scheduled retry time arriving.
	_, err = db.Exec(ctx,
		`UPDATE deliveries
 SET next_attempt_at=now()-interval '1 second'
 WHERE event_id=$1`,
		eventID,
	)
	if err != nil {
		t.Fatal(err)
	}

	second, err := s.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second == nil || second.EventID != eventID {
		t.Fatal("retry claim failed")
	}
	if second.Token == first.Token {
		t.Fatal("retry reused the previous lease token")
	}

	s.deliver(ctx, second)

	var attempts int
	var deliveredAt *time.Time

	err = db.QueryRow(ctx,
		`SELECT state,attempts,delivered_at
 FROM deliveries WHERE event_id=$1`,
		eventID,
	).Scan(&state, &attempts, &deliveredAt)
	if err != nil {
		t.Fatal(err)
	}

	if state != "delivered" {
		t.Errorf("expected delivered, got %s", state)
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
	if deliveredAt == nil {
		t.Error("delivered_at missing")
	}
	if requests.Load() != 2 {
		t.Errorf("expected 2 HTTP requests, got %d", requests.Load())
	}

	t.Logf(
		"Retry succeeded: requests=%d, attempts=%d, state=%s",
		requests.Load(), attempts, state,
	)
}

func TestWebhookMovesToDeadAfterMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	db := isolatedTestDB(t)

	var requests atomic.Int32

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		},
	))
	defer receiver.Close()

	endpointID := uuid()
	eventID := uuid()

	_, err := db.Exec(ctx,
		`INSERT INTO endpoints(id,url,secret)
 VALUES($1,$2,$3)`,
		endpointID, receiver.URL, "dead-letter-secret",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx,
		`INSERT INTO events(id,endpoint_id,event_type,payload)
 VALUES($1,$2,$3,$4)`,
		eventID, endpointID, "dead.test",
		[]byte(`{"failure":true}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx,
		`INSERT INTO deliveries(event_id,attempts)
 VALUES($1,$2)`,
		eventID, maxAttempts-1,
	)
	if err != nil {
		t.Fatal(err)
	}

	s := &server{
		db:     db,
		client: receiver.Client(),
	}

	j, err := s.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if j == nil || j.EventID != eventID {
		t.Fatal("failed to claim expected event")
	}
	if j.Attempts != maxAttempts {
		t.Fatalf("expected attempt %d, got %d",
			maxAttempts, j.Attempts)
	}

	s.deliver(ctx, j)

	var state, lastError string
	var attempts int
	var leaseToken *string

	err = db.QueryRow(ctx,
		`SELECT state,attempts,last_error,lease_token::text
 FROM deliveries WHERE event_id=$1`,
		eventID,
	).Scan(&state, &attempts, &lastError, &leaseToken)
	if err != nil {
		t.Fatal(err)
	}

	if state != "dead" {
		t.Errorf("expected dead, got %s", state)
	}
	if attempts != maxAttempts {
		t.Errorf("expected %d attempts, got %d",
			maxAttempts, attempts)
	}
	if lastError != "HTTP 500" {
		t.Errorf("expected HTTP 500, got %q", lastError)
	}
	if leaseToken != nil {
		t.Error("lease token was not cleared")
	}
	if requests.Load() != 1 {
		t.Errorf("expected one HTTP request, got %d",
			requests.Load())
	}

	// Dead deliveries must not be claimed again.
	again, err := s.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again != nil {
		t.Fatal("dead delivery was claimed again")
	}

	t.Logf(
		"Dead-letter transition verified: state=%s, attempts=%d",
		state, attempts,
	)
}
