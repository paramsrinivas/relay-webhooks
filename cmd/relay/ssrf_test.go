package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWebhookClientBlocksLoopback(t *testing.T) {
	var received atomic.Bool

	receiver := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			received.Store(true)
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer receiver.Close()

	client := newWebhookClient()

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, receiver.URL,
		strings.NewReader(`{"test":true}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Do(req)
	if resp != nil {
		resp.Body.Close()
	}

	if err == nil {
		t.Fatal("SECURITY FAILURE: loopback request was allowed")
	}

	if !strings.Contains(err.Error(), "private destination blocked") {
		t.Fatalf(
			"expected SSRF protection error, got: %v",
			err,
		)
	}

	if received.Load() {
		t.Fatal("SECURITY FAILURE: receiver got the request")
	}

	t.Log("PASS: production transport blocked loopback")
}

func TestWebhookClientBlocksPrivateNetworks(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"IPv4 loopback", "https://127.0.0.1/"},
		{"Private 10 network", "https://10.0.0.1/"},
		{"Private 172 network", "https://172.16.0.1/"},
		{"Private 192 network", "https://192.168.1.1/"},
		{"Cloud metadata", "https://169.254.169.254/"},
		{"IPv6 loopback", "https://[::1]/"},
		{"IPv6 unique local", "https://[fc00::1]/"},
		{"IPv6 link local", "https://[fe80::1]/"},
		{"IPv4 unspecified", "https://0.0.0.0/"},
		{"IPv6 unspecified", "https://[::]/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newWebhookClient()

			ctx, cancel := context.WithTimeout(
				context.Background(), 2*time.Second,
			)
			defer cancel()

			req, err := http.NewRequestWithContext(
				ctx, http.MethodPost, tt.url, nil,
			)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := client.Do(req)
			if resp != nil {
				resp.Body.Close()
			}

			if err == nil {
				t.Fatalf(
					"SECURITY FAILURE: allowed %s",
					tt.url,
				)
			}

			if !strings.Contains(
				err.Error(), "private destination blocked",
			) {
				t.Fatalf(
					"expected SSRF rejection for %s, got: %v",
					tt.url, err,
				)
			}
		})
	}
}
