package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type redirectTestTransport func(*http.Request) (*http.Response, error)

func (f redirectTestTransport) RoundTrip(
	req *http.Request,
) (*http.Response, error) {
	return f(req)
}

func TestWebhookClientDoesNotFollowRedirects(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		location string
	}{
		{
			"Redirect to cloud metadata",
			302,
			"http://169.254.169.254/latest/meta-data/",
		},
		{
			"Redirect to localhost",
			301,
			"http://127.0.0.1/admin",
		},
		{
			"Redirect to another HTTPS host",
			307,
			"https://other.example/webhook",
		},
		{
			"Redirect to same host",
			308,
			"https://receiver.example/other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0

			client := newWebhookClient()

			client.Transport = redirectTestTransport(
				func(req *http.Request) (*http.Response, error) {
					calls++

					if calls > 1 {
						t.Fatal("SECURITY FAILURE: redirect followed")
					}

					return &http.Response{
						StatusCode: tt.status,
						Header: http.Header{
							"Location": []string{tt.location},
						},
						Body: io.NopCloser(
							strings.NewReader("redirect"),
						),
						Request: req,
					}, nil
				},
			)

			req, err := http.NewRequest(
				http.MethodPost,
				"https://receiver.example/webhook",
				strings.NewReader(`{"test":true}`),
			)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if calls != 1 {
				t.Fatalf("expected 1 HTTP request, got %d", calls)
			}

			if resp.StatusCode != tt.status {
				t.Fatalf(
					"expected status %d, got %d",
					tt.status, resp.StatusCode,
				)
			}

			if resp.Header.Get("Location") != tt.location {
				t.Fatal("redirect location was not preserved")
			}
		})
	}
}
