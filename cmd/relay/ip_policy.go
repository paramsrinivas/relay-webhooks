package main

import (
	"net"
	"net/netip"
)

// Conservative denylist of special-purpose address ranges.
// This supplements Go's built-in IP classification.
var blockedWebhookPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

var globalIPv6Range = netip.MustParsePrefix("2000::/3")

func isAllowedWebhookIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}

	// Normalize IPv4-mapped IPv6 addresses.
	addr = addr.Unmap()

	if !addr.IsGlobalUnicast() ||
		addr.IsPrivate() ||
		addr.IsLoopback() ||
		addr.IsLinkLocalUnicast() {
		return false
	}

	// Only accept the currently allocated global IPv6 range.
	if addr.Is6() && !globalIPv6Range.Contains(addr) {
		return false
	}

	for _, prefix := range blockedWebhookPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}

	return true
}
