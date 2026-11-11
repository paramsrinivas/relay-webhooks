package main

import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "testing"
)

func TestSignProducesValidHMAC(t *testing.T) {
    secret := "test-secret"
    timestamp := "1700000000"
    payload := []byte(`{"payment_id":"pay_123"}`)

    got := sign(secret, timestamp, payload)

    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write([]byte(timestamp + "."))
    mac.Write(payload)

    expected := hex.EncodeToString(mac.Sum(nil))

    if got != expected {
        t.Fatalf("signature mismatch: got %s, want %s", got, expected)
    }
}

func TestSignChangesWithPayload(t *testing.T) {
    secret := "test-secret"
    timestamp := "1700000000"

    first := sign(secret, timestamp, []byte(`{"amount":100}`))
    second := sign(secret, timestamp, []byte(`{"amount":200}`))

    if first == second {
        t.Fatal("different payloads produced identical signatures")
    }
}

func TestSignChangesWithTimestamp(t *testing.T) {
    secret := "test-secret"
    payload := []byte(`{"amount":100}`)

    first := sign(secret, "1700000000", payload)
    second := sign(secret, "1700000001", payload)

    if first == second {
        t.Fatal("different timestamps produced identical signatures")
    }
}

func TestSignChangesWithSecret(t *testing.T) {
    timestamp := "1700000000"
    payload := []byte(`{"amount":100}`)

    first := sign("secret-one", timestamp, payload)
    second := sign("secret-two", timestamp, payload)

    if first == second {
        t.Fatal("different secrets produced identical signatures")
    }
}
