# Relay — Go Webhook Delivery Service

A learning/portfolio implementation of a PostgreSQL-backed, at-least-once webhook dispatcher.

## Quick start

```bash
docker compose up -d --build
curl http://localhost:8080/health
```

Register a public HTTPS receiver (the API returns its HMAC secret once):

```bash
curl -X POST http://localhost:8080/endpoints -H 'Content-Type: application/json' \
  -d '{"url":"https://YOUR-PUBLIC-HTTPS-RECEIVER.example/webhook"}'
```

Publish an event, replacing ENDPOINT_ID with the returned ID:

```bash
curl -X POST http://localhost:8080/events -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: payment-123-authorized' \
  -d '{"endpoint_id":"ENDPOINT_ID","type":"payment.allowed","payload":{"payment_id":"pay_123","decision":"ALLOW"}}'
```

Check state with `GET /deliveries/{event_id}`. Replay dead deliveries using `POST /replay/{event_id}`.

## Testing

Requires Go 1.23+ and Docker with Docker Compose.

Start PostgreSQL:

```bash
docker compose up -d postgres
```

Create the dedicated test database if it does not already exist:

```bash
if [ "$(docker compose exec -T postgres psql -U relay -d postgres -Atqc "SELECT 1 FROM pg_database WHERE datname = 'relay_test'")" != "1" ]; then
  docker compose exec -T postgres createdb -U relay relay_test
fi
```

Apply the database migration:

```bash
docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U relay -d relay_test \
  < migrations/001_init.sql
```

Run all tests with Go's race detector:

```bash
go test -race -count=1 ./...
```

For repeated concurrency and reliability checks:

```bash
go test -race -count=10 ./...
go vet ./...
```

Integration tests use PostgreSQL. Most database tests create isolated
schemas in `relay_test` and remove them during cleanup. The application
uses the separate `relay` database.

## Delivery guarantees and limitations

- Events and initial delivery records are inserted in one transaction.
- Idempotency keys are scoped to endpoints. Repeating the same event returns its existing ID; reusing a key with different event content returns HTTP 409 Conflict.
- Workers claim rows using PostgreSQL row locks, `SKIP LOCKED`, 30-second leases, and ownership tokens.
- Requests are signed with HMAC-SHA256 over `timestamp + '.' + raw_body` and include a stable event ID.
- A worker crash after HTTP success but before the DB commit can cause a duplicate request. Receivers **must** deduplicate by event ID.
- Workers retry up to eight attempts and then mark deliveries `dead`; manual replay is supported.
- No ordering guarantee. No exactly-once delivery guarantee.
- Endpoint URLs must use HTTPS and default port 443. Private/loopback addresses are blocked during DNS resolution and dial; redirects are disabled.

## Important before production

This is a first milestone, not production-ready. The API has no authentication, per-tenant authorization, rate limits, or secret encryption. It has no heartbeat for long deliveries (HTTP timeout is 10s, lease is 30s). No per-endpoint concurrency control or Prometheus metrics yet. Multi-tenant security, secret rotation, lifecycle management, and stronger SSRF controls are required.
