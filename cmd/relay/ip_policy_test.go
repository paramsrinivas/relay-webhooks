package main

import (
	"net"
	"testing"
)

func TestWebhookIPPolicy(t *testing.T) {
	tests := []struct {
		name    string
		address string
		allowed bool
	}{
		{"IPv4 loopback", "127.0.0.1", false},
		{"Private IPv4", "10.0.0.1", false},
		{"Cloud metadata", "169.254.169.254", false},
		{"Shared address space", "100.64.0.1", false},
		{"Documentation IPv4 A", "192.0.2.1", false},
		{"Documentation IPv4 B", "198.51.100.1", false},
		{"Documentation IPv4 C", "203.0.113.1", false},
		{"Benchmarking IPv4", "198.18.0.1", false},
		{"Special purpose IPv4", "192.0.0.1", false},
		{"IPv4 multicast", "224.0.0.1", false},
		{"IPv4 reserved", "240.0.0.1", false},
		{"IPv6 loopback", "::1", false},
		{"IPv6 unique local", "fc00::1", false},
		{"IPv6 link local", "fe80::1", false},
		{"Documentation IPv6", "2001:db8::1", false},
		{"Special purpose IPv6", "2001::1", false},
		{"IPv6 6to4", "2002::1", false},
		{"IPv4 mapped loopback", "::ffff:127.0.0.1", false},
		{"Public IPv4 Google", "8.8.8.8", true},
		{"Public IPv4 Cloudflare", "1.1.1.1", true},
		{"Public IPv6 Google", "2001:4860:4860::8888", true},
		{"Public IPv6 Cloudflare", "2606:4700:4700::1111", true},
		{"Invalid address", "not-an-ip", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.address)
			got := isAllowedWebhookIP(ip)

			if got != tt.allowed {
				t.Errorf(
					"IP %s: allowed=%t, want %t",
					tt.address, got, tt.allowed,
				)
			}
		})
	}
}
