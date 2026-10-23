package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/auth"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/db"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/queue"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/ratelimit"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/worker"
)

// These tests drive the full execution lifecycle in-process:
// POST /api/v1/execute → Redis enqueue → worker → go-judge → SSE VERDICT.
// Redis is miniredis, go-judge is an httptest mock, and PostgreSQL is a tiny
// wire-protocol fake, so neither Docker nor a live stack is needed.

func TestExecuteAndStream_Accepted(t *testing.T) {
	verdict := executeAndStream(t, runBody("3", ""))

	if got := verdict["verdict"]; got != "accepted" {
		t.Errorf("verdict = %v, want accepted", got)
	}
	if got := verdict["tests_passed"]; got != float64(1) {
		t.Errorf("tests_passed = %v, want 1", got)
	}
}

func TestExecuteAndStream_WrongAnswer(t *testing.T) {
	verdict := executeAndStream(t, runBody("4", ""))

	if got := verdict["verdict"]; got != "wrong_answer" {
		t.Errorf("verdict = %v, want wrong_answer", got)
	}
	if got := verdict["tests_passed"]; got != float64(0) {
		t.Errorf("tests_passed = %v, want 0", got)
	}
}

func TestExecuteAndStream_FloatChecker(t *testing.T) {
	// The mock judge prints "3": within 1e-6 of 3.0000001, but not an exact match.
	verdict := executeAndStream(t, runBody("3.0000001", "float"))
	if got := verdict["verdict"]; got != "accepted" {
		t.Errorf("float checker: verdict = %v, want accepted", got)
	}

	verdict = executeAndStream(t, runBody("3.0000001", ""))
	if got := verdict["verdict"]; got != "wrong_answer" {
		t.Errorf("default checker: verdict = %v, want wrong_answer", got)
	}
}

// runBody builds a single-test python run request; an empty checker omits the field.
func runBody(expectedOutput, checker string) string {
	c := ""
	if checker != "" {
		c = fmt.Sprintf(`"checker":%q,`, checker)
	}
	return fmt.Sprintf(`{"language":"python","source_code":"print(1+2)",%s"tests":[{"input":"","expected_output":%q}]}`, c, expectedOutput)
}

// executeAndStream POSTs body (a request for a single python test) to /execute
// against a mock judge that always prints "3", streams it over SSE,
// and returns the VERDICT event's data after checking the event sequence.
func executeAndStream(t *testing.T, body string) map[string]any {
	t.Helper()
	baseURL, releaseJudge := startExecutionStack(t)

	resp, err := http.Post(baseURL+"/api/v1/execute", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /execute: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort: test response body
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /execute status = %d, want 201: %s", resp.StatusCode, b)
	}
	var exec executeResponse
	if err := json.NewDecoder(resp.Body).Decode(&exec); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}
	if exec.JobID == "" || exec.StreamToken == "" {
		t.Fatalf("execute response missing job_id or stream_token: %+v", exec)
	}

	// The same job must not stream without its token.
	for _, tok := range []string{"", exec.StreamToken + "x"} {
		q := url.Values{"job_id": {exec.JobID}, "stream_token": {tok}}
		bad, err := http.Get(baseURL + "/api/v1/stream?" + q.Encode())
		if err != nil {
			t.Fatalf("GET /stream with stream_token %q: %v", tok, err)
		}
		_ = bad.Body.Close()
		if bad.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET /stream with stream_token %q: status = %d, want 401", tok, bad.StatusCode)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	q := url.Values{"job_id": {exec.JobID}, "stream_token": {exec.StreamToken}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/stream?"+q.Encode(), nil)
	if err != nil {
		t.Fatalf("build stream request: %v", err)
	}
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /stream: %v", err)
	}
	defer stream.Body.Close() //nolint:errcheck // best-effort: test response body
	if stream.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(stream.Body)
		t.Fatalf("GET /stream status = %d, want 200: %s", stream.StatusCode, b)
	}

	// The body only ends when the server closes the stream, so parse it off
	// the test goroutine and hand events back over a buffered channel.
	events := make(chan eventlog.Event, 16)
	go readSSE(ctx, stream.Body, events)

	var seen []eventlog.EventType
	for {
		select {
		case evt, ok := <-events:
			if !ok {
				t.Fatalf("stream closed before VERDICT; saw %v", seen)
			}
			if evt.JobID != exec.JobID {
				t.Fatalf("event job_id = %q, want %q", evt.JobID, exec.JobID)
			}
			seen = append(seen, evt.Type)
			switch evt.Type {
			case eventlog.EventQueued:
				// QUEUED arrives via log replay, which runs after HandleStream has
				// subscribed. Only now let the judge answer, so the VERDICT can only
				// reach us over the live pub/sub path, never the replay.
				releaseJudge()
			case eventlog.EventVerdict:
				if want := []eventlog.EventType{eventlog.EventQueued, eventlog.EventRunning, eventlog.EventTestResult, eventlog.EventVerdict}; fmt.Sprint(seen) != fmt.Sprint(want) {
					t.Errorf("event sequence = %v, want %v", seen, want)
				}
				data, ok := evt.Data.(map[string]any)
				if !ok {
					t.Fatalf("VERDICT data = %T, want object", evt.Data)
				}
				if got := data["test_count"]; got != float64(1) {
					t.Errorf("test_count = %v, want 1", got)
				}
				results, _ := data["results"].([]any)
				if len(results) != 1 {
					t.Fatalf("results = %v, want 1 entry", data["results"])
				}
				if got := results[0].(map[string]any)["stdout"]; got != "3" {
					t.Errorf("results[0].stdout = %v, want 3", got)
				}
				return data
			}
		case <-ctx.Done():
			t.Fatalf("no VERDICT within 10s; saw %v", seen)
		}
	}
}

// readSSE parses "event:"/"data:" frames from r and sends each decoded event
// on out, closing out when the stream ends. Heartbeat comments are skipped.
func readSSE(ctx context.Context, r io.Reader, out chan<- eventlog.Event) {
	defer close(out)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var data string
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			data = strings.TrimPrefix(line, "data: ")
			continue
		}
		if line != "" || data == "" {
			continue
		}
		var evt eventlog.Event
		if err := json.Unmarshal([]byte(data), &evt); err == nil {
			select {
			case out <- evt:
			case <-ctx.Done():
				return
			}
		}
		data = ""
	}
}

// startExecutionStack wires the real gateway and worker against miniredis, a
// fake PostgreSQL and a mock go-judge. It returns the gateway URL and a func
// that unblocks the judge's /run response.
func startExecutionStack(t *testing.T) (string, func()) {
	t.Helper()
	st := startStack(t, stackOptions{})
	return st.baseURL, st.releaseJudge
}

// stackOptions tweaks the stack built by startStack.
type stackOptions struct {
	wrap    func(http.Handler) http.Handler // wraps the gateway mux (e.g. to inject an identity)
	limiter *ratelimit.Limiter              // per-user limiter; required for authenticated callers
}

type stack struct {
	baseURL      string
	releaseJudge func()
	rdb          *goredis.Client
	pool         *pgxpool.Pool
}

func startStack(t *testing.T, opts stackOptions) *stack {
	t.Helper()

	pool := newFakePGPool(t)
	_, rdb := newTestRedis(t)

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseJudge := func() { releaseOnce.Do(func() { close(release) }) }
	judgeSrv := mockRunJudge(t, release)
	t.Cleanup(releaseJudge) // never leave a /run handler blocked on teardown
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	dc := queue.NewDualConsumer(rdb, submitStream.StreamName(), runStream.StreamName(), "workers", 1, 100*time.Millisecond)
	if err := dc.CreateGroups(context.Background()); err != nil {
		t.Fatalf("create consumer groups: %v", err)
	}
	ps := queue.NewPubSub(rdb)

	mux := http.NewServeMux()
	NewHandler(pool, submitStream, runStream, ps, judge, rdb, opts.limiter, nil, 5000, 1, nil).Register(mux)
	var root http.Handler = mux
	if opts.wrap != nil {
		root = opts.wrap(mux)
	}
	gw := httptest.NewServer(root)
	t.Cleanup(func() {
		gw.Close()
		ps.Wait()
	})

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	worker.New(pool, judge, dc, ps, 10*time.Second, 1, 0).Start(ctx, 1, &wg)
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	return &stack{baseURL: gw.URL, releaseJudge: releaseJudge, rdb: rdb, pool: pool}
}

func TestExecute_AuthenticatedCallerGetsStreamToken(t *testing.T) {
	_, rdb := newTestRedis(t)
	userID := uuid.New()
	st := startStack(t, stackOptions{
		limiter: ratelimit.New(rdb, 1000, 1000),
		wrap: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := auth.WithIdentity(r.Context(), auth.Identity{TenantID: model.DefaultTenantID, UserID: userID})
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		},
	})

	resp, err := http.Post(st.baseURL+"/api/v1/execute", "application/json", strings.NewReader(runBody("3", "")))
	if err != nil {
		t.Fatalf("POST /execute: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort: test response body
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201: %s", resp.StatusCode, b)
	}
	var exec executeResponse
	if err := json.NewDecoder(resp.Body).Decode(&exec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if exec.StreamToken == "" {
		t.Error("authenticated caller got no stream_token")
	}
	n, err := st.rdb.XLen(context.Background(), "ac:submissions:run").Result()
	if err != nil || n == 0 {
		t.Errorf("XLen(ac:submissions:run) = %d, %v; want non-zero", n, err)
	}
}

func TestHandleCancel_UnauthRequiresStreamToken(t *testing.T) {
	st := startStack(t, stackOptions{})

	resp, err := http.Post(st.baseURL+"/api/v1/execute", "application/json", strings.NewReader(runBody("3", "")))
	if err != nil {
		t.Fatalf("POST /execute: %v", err)
	}
	var exec executeResponse
	err = json.NewDecoder(resp.Body).Decode(&exec)
	_ = resp.Body.Close()
	if err != nil || exec.JobID == "" || exec.StreamToken == "" {
		t.Fatalf("execute response: %+v, %v", exec, err)
	}

	cancelURL := st.baseURL + "/api/v1/submissions/" + exec.JobID
	del := func(u string) (int, string) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, u, nil)
		if err != nil {
			t.Fatalf("build DELETE: %v", err)
		}
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("DELETE: %v", err)
		}
		defer r.Body.Close() //nolint:errcheck // best-effort: test response body
		b, _ := io.ReadAll(r.Body)
		return r.StatusCode, string(b)
	}

	code, body := del(cancelURL)
	if code != http.StatusUnauthorized || !strings.Contains(body, "invalid or missing stream_token") {
		t.Errorf("no token: status = %d body = %s, want 401 invalid or missing stream_token", code, body)
	}

	// The worker may already have claimed the job, so 200 and 409 are both correct.
	code, body = del(cancelURL + "?stream_token=" + url.QueryEscape(exec.StreamToken))
	if code == http.StatusUnauthorized {
		t.Errorf("valid token: status = 401, body = %s", body)
	}
	if code != http.StatusOK && code != http.StatusConflict {
		t.Errorf("valid token: status = %d, want 200 or 409: %s", code, body)
	}
}

// mockRunJudge stands in for go-judge. POST /run holds its canned single-test
// response until release is closed.
func mockRunJudge(t *testing.T, release <-chan struct{}) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"buildVersion":"test"}`)
	})
	mux.HandleFunc("POST /run", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[{"status":"Accepted","time":100000,"memory":4096,"files":{"stdout":"3","stderr":""}}]`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// --- fake PostgreSQL ---

// fakePG speaks just enough of the PostgreSQL wire protocol to serve the
// submission statements the gateway and worker issue on the execute path,
// backed by an in-memory table. The pool uses QueryExecModeExec, so every
// statement is a single Parse/Bind/Describe/Execute/Sync round trip with
// text-format parameters and results.
type fakePG struct {
	mu   sync.Mutex
	rows map[string]map[string][]byte // submission id → column → text value (nil = NULL)
}

var submissionColumns = []struct {
	name string
	oid  uint32
}{
	{"id", pgtype.UUIDOID}, {"tenant_id", pgtype.UUIDOID}, {"user_id", pgtype.UUIDOID},
	{"idempotency_key", pgtype.TextOID},
	{"language", pgtype.TextOID}, {"source_code", pgtype.TextOID}, {"status", pgtype.TextOID},
	{"verdict", pgtype.TextOID}, {"compile_stderr", pgtype.TextOID}, {"cpu_time_ns", pgtype.Int8OID},
	{"memory_bytes", pgtype.Int8OID}, {"test_count", pgtype.Int4OID}, {"tests_passed", pgtype.Int4OID},
	{"created_at", pgtype.TimestamptzOID}, {"updated_at", pgtype.TimestamptzOID},
}

// Columns bound by each write, in $1..$n order.
var (
	insertColumns  = []string{"id", "tenant_id", "user_id", "idempotency_key", "language", "source_code", "status"}
	verdictColumns = []string{"id", "status", "verdict", "compile_stderr", "cpu_time_ns", "memory_bytes", "test_count", "tests_passed"}
)

func newFakePGPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fakepg listen: %v", err)
	}
	f := &fakePG{rows: map[string]map[string][]byte{}}

	var wg sync.WaitGroup
	var connsMu sync.Mutex
	var conns []net.Conn
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			connsMu.Lock()
			conns = append(conns, conn)
			connsMu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close() //nolint:errcheck // best-effort: fake server teardown
				f.serve(conn)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		connsMu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		connsMu.Unlock()
		wg.Wait()
	})

	dsn := fmt.Sprintf("postgres://test:test@%s/test?sslmode=disable&default_query_exec_mode=exec", ln.Addr())
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("fakepg pool: %v", err)
	}
	t.Cleanup(pool.Close) // registered last, so it closes before the server stops
	return pool
}

func (f *fakePG) serve(conn net.Conn) {
	be := pgproto3.NewBackend(conn, conn)

	startup, err := be.ReceiveStartupMessage()
	if err != nil {
		return
	}
	if _, ok := startup.(*pgproto3.StartupMessage); !ok {
		return // e.g. a CancelRequest; nothing to cancel here
	}
	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
	be.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	be.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if be.Flush() != nil {
		return
	}

	var sql string
	var params [][]byte
	failed := false // after an error, discard messages until Sync
	for {
		msg, err := be.Receive()
		if err != nil {
			return
		}
		if _, isSync := msg.(*pgproto3.Sync); failed && !isSync {
			continue
		}
		switch m := msg.(type) {
		case *pgproto3.Parse:
			sql = strings.Join(strings.Fields(m.Query), " ")
			be.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Bind:
			params = make([][]byte, len(m.Parameters))
			for i, p := range m.Parameters {
				if p != nil {
					params[i] = bytes.Clone(p) // Receive reuses its buffer
				}
			}
			be.Send(&pgproto3.BindComplete{})
		case *pgproto3.Describe:
			if strings.HasPrefix(sql, "SELECT") {
				be.Send(submissionRowDescription())
			} else {
				be.Send(&pgproto3.NoData{})
			}
		case *pgproto3.Execute:
			rows, tag, err := f.exec(sql, params)
			if err != nil {
				be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "0A000", Message: err.Error()})
				failed = true
				continue
			}
			for _, r := range rows {
				be.Send(&pgproto3.DataRow{Values: r})
			}
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
		case *pgproto3.Sync:
			failed = false
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if be.Flush() != nil {
				return
			}
		case *pgproto3.Query: // pgxpool's liveness ping ("-- ping")
			be.Send(&pgproto3.EmptyQueryResponse{})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if be.Flush() != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		}
	}
}

func submissionRowDescription() *pgproto3.RowDescription {
	fields := make([]pgproto3.FieldDescription, len(submissionColumns))
	for i, c := range submissionColumns {
		fields[i] = pgproto3.FieldDescription{Name: []byte(c.name), DataTypeOID: c.oid, DataTypeSize: -1, TypeModifier: -1}
	}
	return &pgproto3.RowDescription{Fields: fields}
}

// exec runs one normalised statement against the in-memory table.
func (f *fakePG) exec(sql string, params [][]byte) (rows [][][]byte, tag string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case strings.HasPrefix(sql, "INSERT INTO submissions "):
		now := []byte(time.Now().UTC().Format("2006-01-02 15:04:05.999999") + "+00")
		row := map[string][]byte{"created_at": now, "updated_at": now}
		for i, col := range insertColumns {
			row[col] = params[i]
		}
		f.rows[string(params[0])] = row
		return nil, "INSERT 0 1", nil

	case strings.HasPrefix(sql, "SELECT id, tenant_id,") && strings.HasSuffix(sql, "FROM submissions WHERE id = $1"):
		row, ok := f.rows[string(params[0])]
		if !ok {
			return nil, "SELECT 0", nil
		}
		vals := make([][]byte, len(submissionColumns))
		for i, c := range submissionColumns {
			vals[i] = row[c.name]
		}
		return [][][]byte{vals}, "SELECT 1", nil

	case strings.HasPrefix(sql, "UPDATE submissions SET status = $2, updated_at = now()"):
		return nil, f.updateNonTerminal(params, []string{"id", "status"}), nil

	case strings.HasPrefix(sql, "UPDATE submissions SET status = $2, verdict = $3,"):
		return nil, f.updateNonTerminal(params, verdictColumns), nil

	case strings.HasPrefix(sql, "UPDATE submissions SET status = 'failed', verdict = 'cancelled'"):
		row, ok := f.rows[string(params[0])]
		if !ok || string(row["status"]) != "queued" {
			return nil, "UPDATE 0", nil
		}
		row["status"] = []byte("failed")
		row["verdict"] = []byte("cancelled")
		return nil, "UPDATE 1", nil

	case strings.HasPrefix(sql, "INSERT INTO submission_test_results "):
		return nil, "INSERT 0 1", nil
	}
	return nil, "", fmt.Errorf("fakepg: unsupported statement: %s", sql)
}

// updateNonTerminal applies params to cols[1:] of the row whose id is params[0],
// honouring the "status NOT IN ('completed', 'failed')" guard.
func (f *fakePG) updateNonTerminal(params [][]byte, cols []string) string {
	row, ok := f.rows[string(params[0])]
	if !ok {
		return "UPDATE 0"
	}
	if s := string(row["status"]); s == "completed" || s == "failed" {
		return "UPDATE 0"
	}
	for i := 1; i < len(cols); i++ {
		row[cols[i]] = params[i]
	}
	return "UPDATE 1"
}

// A job enqueued by an older release (submit stream, problem-bank tests, no
// ac:tests: hash) must fail cleanly instead of hanging or panicking.
func TestInFlightLegacySubmitJob_FailsCleanly(t *testing.T) {
	st := startStack(t, stackOptions{})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	jobID := uuid.New()
	if err := db.InsertSubmission(ctx, st.pool, &model.Submission{
		ID:         jobID,
		TenantID:   model.DefaultTenantID,
		Language:   "python",
		SourceCode: "print(3)",
		Status:     model.StatusQueued,
	}); err != nil {
		t.Fatalf("insert submission: %v", err)
	}
	token, err := newStreamToken(ctx, st.rdb, jobID.String())
	if err != nil {
		t.Fatalf("stream token: %v", err)
	}
	if err := st.rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: "ac:submissions:submit",
		Values: map[string]any{"submission_id": jobID.String()},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	q := url.Values{"job_id": {jobID.String()}, "stream_token": {token}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, st.baseURL+"/api/v1/stream?"+q.Encode(), nil)
	if err != nil {
		t.Fatalf("build stream request: %v", err)
	}
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /stream: %v", err)
	}
	defer stream.Body.Close() //nolint:errcheck // best-effort: test response body
	if stream.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(stream.Body)
		t.Fatalf("GET /stream status = %d, want 200: %s", stream.StatusCode, b)
	}

	events := make(chan eventlog.Event, 16)
	go readSSE(ctx, stream.Body, events)
	for {
		select {
		case evt, ok := <-events:
			if !ok {
				t.Fatal("stream closed before VERDICT")
			}
			if evt.Type != eventlog.EventVerdict {
				continue
			}
			data, _ := evt.Data.(map[string]any)
			if got := data["verdict"]; got != "internal_error" {
				t.Errorf("verdict = %v, want internal_error", got)
			}
			return
		case <-ctx.Done():
			t.Fatal("no VERDICT within 10s")
		}
	}
}
