package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestWebhookClientRejectsUnsafeDNS(t *testing.T) {
	tests := []struct {
		name string
		ips  []string
	}{
		{
			"Public then private",
			[]string{"8.8.8.8", "10.0.0.5"},
		},
		{
			"Private then public",
			[]string{"10.0.0.5", "8.8.8.8"},
		},
		{
			"Shared address space",
			[]string{"8.8.8.8", "100.64.0.1"},
		},
		{
			"IPv6 documentation range",
			[]string{"2001:4860:4860::8888", "2001:db8::1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dialCalled atomic.Bool
			var lookupCalled atomic.Bool

			lookup := func(
				ctx context.Context,
				host string,
			) ([]net.IPAddr, error) {
				lookupCalled.Store(true)

				if host != "receiver.example" {
					return nil, errors.New("unexpected hostname")
				}

				var results []net.IPAddr
				for _, address := range tt.ips {
					results = append(results, net.IPAddr{
						IP: net.ParseIP(address),
					})
				}

				return results, nil
			}

			dial := func(
				ctx context.Context,
				network string,
				address string,
			) (net.Conn, error) {
				dialCalled.Store(true)
				return nil, errors.New("unexpected TCP dial")
			}

			client := newWebhookClientWithNetwork(
				lookup, dial,
			)

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

			if !lookupCalled.Load() {
				t.Fatal("DNS resolver was not called")
			}

			if dialCalled.Load() {
				t.Fatal("SECURITY FAILURE: TCP dial attempted")
			}

			if err == nil {
				t.Fatal("SECURITY FAILURE: request allowed")
			}

			if !strings.Contains(
				err.Error(),
				"private destination blocked",
			) {
				t.Fatalf(
					"expected SSRF rejection, got: %v",
					err,
				)
			}
		})
	}
}
