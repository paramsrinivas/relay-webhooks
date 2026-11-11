CREATE TABLE IF NOT EXISTS endpoints (
 id UUID PRIMARY KEY, url TEXT NOT NULL, secret TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS events (
 id UUID PRIMARY KEY, endpoint_id UUID NOT NULL REFERENCES endpoints(id), event_type TEXT NOT NULL,
 payload JSONB NOT NULL, idempotency_key TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(endpoint_id,idempotency_key)
);
CREATE TABLE IF NOT EXISTS deliveries (
 event_id UUID PRIMARY KEY REFERENCES events(id), state TEXT NOT NULL DEFAULT 'pending'
 CHECK (state IN ('pending','leased','delivered','dead')),
 attempts INT NOT NULL DEFAULT 0, next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 lease_until TIMESTAMPTZ, lease_token UUID, last_error TEXT, delivered_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS deliveries_due_idx ON deliveries(next_attempt_at) WHERE state IN ('pending','leased');
