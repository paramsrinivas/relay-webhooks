package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestWebhookClientDialsValidatedIP(t *testing.T) {
	var lookups atomic.Int32
	var dials atomic.Int32

	stopDial := errors.New("simulated dial stopped")
	dialedAddress := make(chan string, 1)

	lookup := func(
		ctx context.Context,
		host string,
	) ([]net.IPAddr, error) {
		if host != "receiver.example" {
			return nil, errors.New("unexpected hostname")
		}

		if lookups.Add(1) == 1 {
			return []net.IPAddr{
				{IP: net.ParseIP("8.8.8.8")},
			}, nil
		}

		// Simulate a malicious second DNS response.
		return []net.IPAddr{
			{IP: net.ParseIP("10.0.0.5")},
		}, nil
	}

	dial := func(
		ctx context.Context,
		network string,
		address string,
	) (net.Conn, error) {
		dials.Add(1)
		dialedAddress <- address
		return nil, stopDial
	}

	client := newWebhookClientWithNetwork(lookup, dial)

	req, err := http.NewRequest(
		http.MethodPost,
		"https://receiver.example/webhook",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Do(req)
	if resp != nil {
		resp.Body.Close()
	}

	if !errors.Is(err, stopDial) {
		t.Fatalf("expected simulated dial error, got: %v", err)
	}

	if lookups.Load() != 1 {
		t.Fatalf("DNS looked up %d times; expected 1", lookups.Load())
	}

	if dials.Load() != 1 {
		t.Fatalf("TCP dialed %d times; expected 1", dials.Load())
	}

	select {
	case address := <-dialedAddress:
		if address != "8.8.8.8:443" {
			t.Fatalf("unexpected dial target: %s", address)
		}
	default:
		t.Fatal("TCP dialer was never called")
	}

	t.Log("PASS: dial used validated IP without re-resolving DNS")
}
