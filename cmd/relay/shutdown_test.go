package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestWorkerFinishesActiveDeliveryOnShutdown(t *testing.T) {
	db := isolatedTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	released := false

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			started <- struct{}{}
			<-release
			w.WriteHeader(http.StatusOK)
		},
	))

	defer func() {
		if !released {
			close(release)
		}
		receiver.Close()
	}()

	endpointID := uuid()
	eventID := uuid()

	_, err := db.Exec(ctx,
		`INSERT INTO endpoints(id,url,secret) VALUES($1,$2,$3)`,
		endpointID, receiver.URL, "shutdown-test-secret",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx,
		`INSERT INTO events(id,endpoint_id,event_type,payload)
 VALUES($1,$2,$3,$4)`,
		eventID, endpointID, "test.shutdown", []byte(`{"ok":true}`),
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

	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()

	var wg sync.WaitGroup
	wg.Add(1)

	go s.worker(workerCtx, 0, &wg)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait until the receiver has the webhook.
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("webhook receiver was never called")
	}

	// Begin shutdown while the receiver is still blocked.
	stopWorker()

	// The worker must not abandon the active delivery.
	select {
	case <-done:
		t.Fatal("worker exited before active delivery completed")
	case <-time.After(150 * time.Millisecond):
	}

	// Allow the receiver to finish successfully.
	close(release)
	released = true

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("worker did not finish after receiver responded")
	}

	var state string
	var attempts int
	var deliveredAt *time.Time

	err = db.QueryRow(ctx,
		`SELECT state, attempts, delivered_at
 FROM deliveries WHERE event_id=$1`,
		eventID,
	).Scan(&state, &attempts, &deliveredAt)

	if err != nil {
		t.Fatal(err)
	}

	if state != "delivered" {
		t.Fatalf("expected delivered, got %s", state)
	}

	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}

	if deliveredAt == nil {
		t.Fatal("delivered_at was not recorded")
	}

	t.Log("active delivery completed successfully during shutdown")
}
