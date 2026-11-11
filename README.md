# Relay — Go Webhook Delivery Service

A learning/portfolio implementation of a PostgreSQL-backed, at-least-once webhook dispatcher.

## Quick start

```bash
docker compose up --build
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

## Delivery guarantees and limitations

- Events and initial delivery records are inserted in one transaction.
- Workers claim rows using PostgreSQL row locks, `SKIP LOCKED`, 30-second leases, and ownership tokens.
- Requests are signed with HMAC-SHA256 over `timestamp + '.' + raw_body` and include a stable event ID.
- A worker crash after HTTP success but before the DB commit can cause a duplicate request. Receivers **must** deduplicate by event ID.
- Workers retry up to eight attempts and then mark deliveries `dead`; manual replay is supported.
- No ordering guarantee. No exactly-once delivery guarantee.
- Endpoint URLs must use HTTPS and default port 443. Private/loopback addresses are blocked during DNS resolution and dial; redirects are disabled.

## Important before production

This is a first milestone, not production-ready. The API has no authentication, per-tenant authorization, rate limits, or secret encryption. It has no heartbeat for long deliveries (HTTP timeout is 10s, lease is 30s). No per-endpoint concurrency control, Prometheus metrics, or automated integration tests yet. Idempotency keys currently return the existing event even if a later request with the same key has a different body; production code should detect that conflict. Multi-tenant security, secret rotation, lifecycle management, and stronger SSRF controls are required.
