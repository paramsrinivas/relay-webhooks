package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxAttempts = 8

type server struct {
	db     *pgxpool.Pool
	client *http.Client
}
type endpointInput struct {
	URL string `json:"url"`
}
type eventInput struct {
	EndpointID string          `json:"endpoint_id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
}
type job struct {
	EventID, URL, Secret, Type string
	Payload                    []byte
	Attempts                   int
	Token                      string
}

func uuid() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	x := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", x[:8], x[8:12], x[12:16], x[16:20], x[20:])
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		respond(w, 400, map[string]string{"error": err.Error()})
		return false
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		respond(w, 400, map[string]string{"error": "expected one JSON object"})
		return false
	}
	return true
}
func validURL(s string) bool {
	u, e := url.Parse(s)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Port() != "" {
		return false
	}
	return true
}
func (s *server) endpoints(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	var in endpointInput
	if !decode(w, r, &in) {
		return
	}
	if !validURL(in.URL) {
		respond(w, 400, map[string]string{"error": "public HTTPS URL without custom port required"})
		return
	}
	id := uuid()
	secret := uuid() + uuid()
	_, err := s.db.Exec(r.Context(), "INSERT INTO endpoints(id,url,secret) VALUES($1,$2,$3)", id, in.URL, secret)
	if err != nil {
		http.Error(w, "database error", 500)
		return
	}
	respond(w, 201, map[string]string{"id": id, "secret": secret})
}
func (s *server) events(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	var in eventInput
	if !decode(w, r, &in) {
		return
	}
	if in.Type == "" || len(in.Payload) == 0 || !json.Valid(in.Payload) {
		respond(w, 400, map[string]string{"error": "type and valid payload required"})
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 200 {
		respond(w, 400, map[string]string{"error": "idempotency key too long"})
		return
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		http.Error(w, "database error", 500)
		return
	}
	defer tx.Rollback(r.Context())
	id := uuid()
	var existing string
	// Idempotent event publishing. A key is scoped to an endpoint.
	err = tx.QueryRow(r.Context(), `INSERT INTO events(id,endpoint_id,event_type,payload,idempotency_key) VALUES($1,$2,$3,$4,$5) ON CONFLICT(endpoint_id,idempotency_key) DO UPDATE SET id=events.id
WHERE events.event_type=EXCLUDED.event_type
AND events.payload=EXCLUDED.payload
RETURNING id`, id, in.EndpointID, in.Type, in.Payload, nullable(key)).Scan(&existing)
	if errors.Is(err, pgx.ErrNoRows) {
		respond(w, 409, map[string]string{"error": "idempotency key reused with different event content"})
		return
	}
	if err != nil {
		respond(w, 400, map[string]string{"error": "invalid endpoint or event"})
		return
	}
	_, err = tx.Exec(r.Context(), "INSERT INTO deliveries(event_id) VALUES($1) ON CONFLICT DO NOTHING", existing)
	if err != nil {
		http.Error(w, "database error", 500)
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		http.Error(w, "database error", 500)
		return
	}
	respond(w, 202, map[string]string{"event_id": existing, "status": "accepted"})
}
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func (s *server) status(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", 405)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/deliveries/")
	var state string
	var attempts int
	var last *string
	err := s.db.QueryRow(r.Context(), "SELECT state,attempts,last_error FROM deliveries WHERE event_id=$1", id).Scan(&state, &attempts, &last)
	if err != nil {
		http.Error(w, "not found", 404)
		return
	}
	respond(w, 200, map[string]any{"event_id": id, "state": state, "attempts": attempts, "last_error": last})
}
func (s *server) replay(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/replay/"), "/")
	tag, err := s.db.Exec(r.Context(), "UPDATE deliveries SET state='pending',attempts=0,next_attempt_at=now(),lease_until=NULL,lease_token=NULL,last_error=NULL WHERE event_id=$1 AND state='dead'", id)
	if err != nil || tag.RowsAffected() == 0 {
		http.Error(w, "dead-letter delivery not found", 404)
		return
	}
	respond(w, 200, map[string]string{"status": "requeued"})
}
func (s *server) claim(ctx context.Context) (*job, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	j := &job{}
	j.Token = uuid()
	err = tx.QueryRow(ctx, `SELECT d.event_id,e.url,e.secret,v.event_type,v.payload,d.attempts FROM deliveries d JOIN events v ON v.id=d.event_id JOIN endpoints e ON e.id=v.endpoint_id WHERE (d.state='pending' AND d.next_attempt_at<=now()) OR (d.state='leased' AND d.lease_until<now()) ORDER BY d.next_attempt_at FOR UPDATE OF d SKIP LOCKED LIMIT 1`).Scan(&j.EventID, &j.URL, &j.Secret, &j.Type, &j.Payload, &j.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, tx.Commit(ctx)
	}
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "UPDATE deliveries SET state='leased',lease_token=$2,lease_until=now()+interval '30 seconds',attempts=attempts+1 WHERE event_id=$1", j.EventID, j.Token)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	j.Attempts++
	return j, nil
}
func sign(secret, stamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(stamp + "."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
func (s *server) deliver(ctx context.Context, j *job) {
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	req, err := http.NewRequestWithContext(ctx, "POST", j.URL, strings.NewReader(string(j.Payload)))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Relay-Event-ID", j.EventID)
		req.Header.Set("X-Relay-Event-Type", j.Type)
		req.Header.Set("X-Relay-Signature", "t="+stamp+",v1="+sign(j.Secret, stamp, j.Payload))
		var res *http.Response
		res, err = s.client.Do(req)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
			res.Body.Close()
			if res.StatusCode >= 200 && res.StatusCode < 300 {
				commitCtx, commitCancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer commitCancel()
				tag, e := s.db.Exec(commitCtx, "UPDATE deliveries SET state='delivered',delivered_at=now(),lease_until=NULL,lease_token=NULL,last_error=NULL WHERE event_id=$1 AND lease_token=$2 AND state='leased'", j.EventID, j.Token)
				if e != nil {
					log.Printf("commit success failed: %v", e)
				} else if tag.RowsAffected() == 0 {
					log.Printf("stale success ignored: event=%s", j.EventID)
				}
				return
			}
			err = fmt.Errorf("HTTP %d", res.StatusCode)
		}
	}
	if err == nil {
		err = errors.New("unknown failure")
	}
	state := "pending"
	if j.Attempts >= maxAttempts {
		state = "dead"
	}
	delay := time.Duration(math.Min(300, math.Pow(2, float64(j.Attempts)))) * time.Second
	random, _ := rand.Int(rand.Reader, big.NewInt(1000))
	delay += time.Duration(random.Int64()) * time.Millisecond
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer commitCancel()
	tag, e := s.db.Exec(commitCtx, "UPDATE deliveries SET state=$3,next_attempt_at=now()+$4::interval,lease_until=NULL,lease_token=NULL,last_error=$5 WHERE event_id=$1 AND lease_token=$2 AND state='leased'", j.EventID, j.Token, state, fmt.Sprintf("%f seconds", delay.Seconds()), err.Error())
	if e != nil {
		log.Printf("record failure failed: %v", e)
	} else if tag.RowsAffected() == 0 {
		log.Printf("stale failure ignored: event=%s", j.EventID)
	}
}
func (s *server) worker(ctx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		job, err := s.claim(ctx)
		if err != nil {
			log.Printf("worker %d claim: %v", id, err)
			time.Sleep(time.Second)
			continue
		}
		if job == nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		deliveryCtx, deliveryCancel := context.WithTimeout(
			context.Background(), 12*time.Second,
		)
		s.deliver(deliveryCtx, job)
		deliveryCancel()
	}
}
func newWebhookClient() *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return newWebhookClientWithNetwork(
		net.DefaultResolver.LookupIPAddr,
		dialer.DialContext,
	)
}

func newWebhookClientWithNetwork(
	lookup func(context.Context, string) ([]net.IPAddr, error),
	dial func(context.Context, string, string) (net.Conn, error),
) *http.Client {
	// Reject private/loopback destination IPs at connection time, including DNS rebinding.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, e
		}
		ips, e := lookup(ctx, host)
		if e != nil {
			return nil, e
		}
		for _, ip := range ips {
			if !isAllowedWebhookIP(ip.IP) {
				return nil, errors.New("private destination blocked")
			}
		}
		if len(ips) == 0 {
			return nil, errors.New("no destination addresses")
		}
		return dial(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}
	return &http.Client{Timeout: 10 * time.Second, Transport: transport, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://relay:relay@localhost:5432/relay?sslmode=disable"
	}
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err = db.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	s := &server{db: db, client: newWebhookClient()}
	n := 4
	if x, e := strconv.Atoi(os.Getenv("WORKERS")); e == nil && x > 0 && x <= 100 {
		n = x
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go s.worker(ctx, i, &wg)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("/endpoints", s.endpoints)
	mux.HandleFunc("/events", s.events)
	mux.HandleFunc("/deliveries/", s.status)
	mux.HandleFunc("/replay/", s.replay)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("relay listening on :%s with %d workers", port, n)
	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- httpServer.ListenAndServe()
	}()

	select {
	case <-signalCtx.Done():
		log.Println("shutdown signal received")
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}

	// Stop accepting HTTP requests.
	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(), 15*time.Second,
	)
	defer shutdownCancel()

	// Stop workers from claiming new deliveries first.
	cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown: %v", err)
	}

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		log.Println("all workers stopped")
	case <-shutdownCtx.Done():
		log.Println("worker shutdown deadline exceeded")
	}
}
