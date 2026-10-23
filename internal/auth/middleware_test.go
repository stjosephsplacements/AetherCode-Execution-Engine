package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

// ── helpers ────────────────────────────────────────────────────────────────

func genRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return key
}

// buildJWT signs a token with the given key and claims.
func buildJWT(t *testing.T, key *rsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("sign JWT: %v", err)
	}
	return s
}

// validClaims returns a default-valid MapClaims set.
func validClaims(issuer, audience, sub string) jwt.MapClaims {
	return jwt.MapClaims{
		"iss":   issuer,
		"aud":   jwt.ClaimStrings{audience},
		"sub":   sub,
		"email": "test@example.com",
		"name":  "Test User",
		"exp":   jwt.NewNumericDate(time.Now().Add(time.Hour)),
		"iat":   jwt.NewNumericDate(time.Now()),
	}
}

// b64url encodes bytes as unpadded base64url.
func b64url(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

// rsaJWKSJSON returns a minimal JWKS JSON containing the RSA public key.
func rsaJWKSJSON(t *testing.T, key *rsa.PrivateKey) json.RawMessage {
	t.Helper()
	pub := key.Public().(*rsa.PublicKey)
	eBytes := new(big.Int).SetInt64(int64(pub.E)).Bytes() //nolint:gosec // test only
	raw := fmt.Sprintf(
		`{"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":"test-key","n":%q,"e":%q}]}`,
		b64url(pub.N.Bytes()), b64url(eBytes),
	)
	return json.RawMessage(raw)
}

// testMiddlewareWithKeyfunc builds a Middleware backed by a static in-memory JWKS derived
// from the provided RSA key. No HTTP server or network call is made.
func testMiddlewareWithKeyfunc(t *testing.T, issuer, audience string, key *rsa.PrivateKey) *Middleware {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	k, err := keyfunc.NewJWKSetJSON(rsaJWKSJSON(t, key))
	if err != nil {
		t.Fatalf("keyfunc.NewJWKSetJSON: %v", err)
	}

	tc, uc := initCaches(ctx)
	return &Middleware{
		jwks:        k,
		jwksCancel:  cancel,
		issuer:      issuer,
		audience:    audience,
		pool:        nil,
		tenantCache: tc,
		userCache:   uc,
	}
}

// ── extractBearerToken ──────────────────────────────────────────────────────

func TestExtractBearerToken(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   string
	}{
		{"present", "Bearer my-token", "my-token"},
		{"absent", "", ""},
		{"no space", "Bearermy-token", ""},
		{"wrong scheme", "Basic dXNlcjpwYXNz", ""},
		{"bearer lowercase", "bearer my-token", "my-token"},
		{"multiple spaces", "Bearer  my-token", " my-token"}, // splits on first space only
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			got := extractBearerToken(req)
			if got != tc.want {
				t.Errorf("extractBearerToken = %q, want %q", got, tc.want)
			}
		})
	}
}

// ── claimString ─────────────────────────────────────────────────────────────

func TestClaimString(t *testing.T) {
	claims := jwt.MapClaims{
		"present":  "hello",
		"wrong":    42,
		"nilvalue": nil,
	}
	if got := claimString(claims, "present"); got != "hello" {
		t.Errorf("present = %q, want %q", got, "hello")
	}
	if got := claimString(claims, "absent"); got != "" {
		t.Errorf("absent = %q, want empty", got)
	}
	if got := claimString(claims, "wrong"); got != "" {
		t.Errorf("wrong type = %q, want empty", got)
	}
	if got := claimString(claims, "nilvalue"); got != "" {
		t.Errorf("nil value = %q, want empty", got)
	}
}

// ── ttlCache ─────────────────────────────────────────────────────────────────

func TestTTLCache_HitMissExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := newTTLCache[uuid.UUID](ctx, 100*time.Millisecond)
	id := uuid.New()

	// Miss before set
	if _, ok := c.get("k1"); ok {
		t.Fatal("expected miss before set")
	}

	c.set("k1", id)

	// Hit immediately
	if got, ok := c.get("k1"); !ok || got != id {
		t.Errorf("hit: got %v ok=%v, want %v", got, ok, id)
	}

	// Expired after TTL
	time.Sleep(150 * time.Millisecond)
	if _, ok := c.get("k1"); ok {
		t.Error("expected cache miss after TTL expiry")
	}
}

func TestTTLCache_Overwrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := newTTLCache[uuid.UUID](ctx, time.Minute)
	id1, id2 := uuid.New(), uuid.New()
	c.set("k", id1)
	c.set("k", id2)
	got, ok := c.get("k")
	if !ok || got != id2 {
		t.Errorf("overwrite: got %v, want %v", got, id2)
	}
}

// ── Identity context round-trip ───────────────────────────────────────────

func TestWithGetIdentity(t *testing.T) {
	ctx := context.Background()
	id := Identity{
		TenantID: model.DefaultTenantID,
		UserID:   uuid.New(),
		Role:     "student",
	}
	ctx = WithIdentity(ctx, id)
	got, ok := GetIdentity(ctx)
	if !ok {
		t.Fatal("GetIdentity: expected ok")
	}
	if got.UserID != id.UserID {
		t.Errorf("UserID = %v, want %v", got.UserID, id.UserID)
	}
	if got.Role != "student" {
		t.Errorf("Role = %q, want student", got.Role)
	}

	_, ok2 := GetIdentity(context.Background())
	if ok2 {
		t.Fatal("GetIdentity on empty ctx should return false")
	}
}

// ── Middleware.Wrap — no token passes through ────────────────────────────────

func TestMiddlewareWrap_NoToken_PassesThrough(t *testing.T) {
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, uc := initCaches(cancelCtx)
	mw := &Middleware{
		jwks:        nil, // never called when no token
		jwksCancel:  cancel,
		issuer:      "https://sso.test",
		audience:    "myapp",
		tenantCache: tc,
		userCache:   uc,
	}

	called := false
	handler := mw.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		_, ok := GetIdentity(r.Context())
		if ok {
			t.Error("identity should not be set when no token is provided")
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !called {
		t.Fatal("inner handler was not called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

// ── Middleware.Wrap — invalid token returns 401 ──────────────────────────────

func TestMiddlewareWrap_InvalidToken_Returns401(t *testing.T) {
	key := genRSAKey(t)
	mw := testMiddlewareWithKeyfunc(t, "https://sso.test", "myapp", key)

	called := false
	handler := mw.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/execute", nil)
	req.Header.Set("Authorization", "Bearer this.is.not.a.valid.jwt")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if called {
		t.Fatal("inner handler should not be called on invalid token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

// ── validateAndResolve — role mapping via Zitadel claims ────────────────────

func TestValidateAndResolve_RoleMapping(t *testing.T) {
	const issuer = "https://sso.test"
	const audience = "myapp"
	key := genRSAKey(t)
	mw := testMiddlewareWithKeyfunc(t, issuer, audience, key)

	// Pre-seed user cache so resolveUser doesn't hit the DB.
	userID := uuid.New()
	cacheKey := model.DefaultTenantID.String() + ":test-sub"
	mw.userCache.set(cacheKey, userID)

	cases := []struct {
		name     string
		roles    interface{}
		wantRole string
	}{
		{"no roles claim", nil, "student"},
		{"empty roles", map[string]interface{}{}, "student"},
		{"faculty", map[string]interface{}{"faculty": map[string]interface{}{}}, "faculty"},
		{"admin", map[string]interface{}{"admin": map[string]interface{}{}}, "admin"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := validClaims(issuer, audience, "test-sub")
			if tc.roles != nil {
				claims["urn:zitadel:iam:org:project:roles"] = tc.roles
			}
			tok := buildJWT(t, key, claims)
			identity, err := mw.validateAndResolve(context.Background(), tok)
			if err != nil {
				t.Fatalf("validateAndResolve: %v", err)
			}
			if identity.Role != tc.wantRole {
				t.Errorf("Role = %q, want %q", identity.Role, tc.wantRole)
			}
		})
	}
}

func TestValidateAndResolve_ClaimsExtraction(t *testing.T) {
	const issuer = "https://sso.test"
	const audience = "myapp"
	key := genRSAKey(t)
	mw := testMiddlewareWithKeyfunc(t, issuer, audience, key)

	userID := uuid.New()
	mw.userCache.set(model.DefaultTenantID.String()+":user-sub-123", userID)

	claims := validClaims(issuer, audience, "user-sub-123")
	claims["email"] = "user@example.com"
	claims["name"] = "Alice"

	identity, err := mw.validateAndResolve(context.Background(), buildJWT(t, key, claims))
	if err != nil {
		t.Fatalf("validateAndResolve: %v", err)
	}
	if identity.ExternalSub != "user-sub-123" {
		t.Errorf("ExternalSub = %q, want %q", identity.ExternalSub, "user-sub-123")
	}
	if identity.Email != "user@example.com" {
		t.Errorf("Email = %q, want %q", identity.Email, "user@example.com")
	}
	if identity.TenantID != model.DefaultTenantID {
		t.Errorf("TenantID = %v, want DefaultTenantID", identity.TenantID)
	}
}

func TestValidateAndResolve_ExpiredToken(t *testing.T) {
	key := genRSAKey(t)
	mw := testMiddlewareWithKeyfunc(t, "https://sso.test", "myapp", key)

	claims := validClaims("https://sso.test", "myapp", "sub")
	claims["exp"] = jwt.NewNumericDate(time.Now().Add(-time.Hour)) // expired
	tok := buildJWT(t, key, claims)

	_, err := mw.validateAndResolve(context.Background(), tok)
	if err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
}

func TestValidateAndResolve_WrongIssuer(t *testing.T) {
	key := genRSAKey(t)
	mw := testMiddlewareWithKeyfunc(t, "https://correct.issuer", "myapp", key)

	claims := validClaims("https://wrong.issuer", "myapp", "sub")
	tok := buildJWT(t, key, claims)

	_, err := mw.validateAndResolve(context.Background(), tok)
	if err == nil {
		t.Fatal("expected error for wrong issuer, got nil")
	}
}

func TestValidateAndResolve_WrongAudience(t *testing.T) {
	key := genRSAKey(t)
	mw := testMiddlewareWithKeyfunc(t, "https://sso.test", "correct-audience", key)

	claims := validClaims("https://sso.test", "wrong-audience", "sub")
	tok := buildJWT(t, key, claims)

	_, err := mw.validateAndResolve(context.Background(), tok)
	if err == nil {
		t.Fatal("expected error for wrong audience, got nil")
	}
}

func TestValidateAndResolve_MissingSub(t *testing.T) {
	key := genRSAKey(t)
	mw := testMiddlewareWithKeyfunc(t, "https://sso.test", "myapp", key)

	claims := validClaims("https://sso.test", "myapp", "")
	delete(claims, "sub") // explicitly absent
	tok := buildJWT(t, key, claims)

	_, err := mw.validateAndResolve(context.Background(), tok)
	if err == nil {
		t.Fatal("expected error for missing sub, got nil")
	}
}

// newUnreachablePGPool creates a pgxpool that succeeds at construction (lazy
// connections) but always fails at query time. The pool connects to 127.0.0.1:1
// which is unreachable, simulating an unknown-org DB lookup failure.
func newUnreachablePGPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://test:test@127.0.0.1:1/test")
	if err != nil {
		t.Fatalf("parse pgxpool config: %v", err)
	}
	cfg.MinConns = 0
	cfg.MaxConns = 1
	cfg.ConnConfig.ConnectTimeout = 100 * time.Millisecond
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestValidateAndResolve_UnregisteredOrgID guards against the cross-tenant
// privilege-escalation described in finding F6: a JWT bearing an
// urn:zitadel:iam:org:id claim that cannot be resolved to a known, active
// tenant must be rejected with an error rather than silently falling back to
// DefaultTenantID.
func TestValidateAndResolve_UnregisteredOrgID(t *testing.T) {
	const issuer = "https://sso.test"
	const audience = "myapp"
	key := genRSAKey(t)

	// Build a middleware backed by a pool that is deliberately unreachable so
	// that resolveTenant always returns an error (simulating pgx.ErrNoRows or
	// a connection failure — both must be treated the same way).
	pool := newUnreachablePGPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	k, err := keyfunc.NewJWKSetJSON(rsaJWKSJSON(t, key))
	if err != nil {
		t.Fatalf("keyfunc.NewJWKSetJSON: %v", err)
	}
	tc, uc := initCaches(ctx)
	mw := &Middleware{
		jwks:        k,
		jwksCancel:  cancel,
		issuer:      issuer,
		audience:    audience,
		pool:        pool,
		tenantCache: tc,
		userCache:   uc,
	}

	// JWT carries a well-formed but unregistered org ID claim.
	claims := validClaims(issuer, audience, "attacker-sub")
	claims["urn:zitadel:iam:org:id"] = "unregistered-org-xyz"
	tok := buildJWT(t, key, claims)

	_, err = mw.validateAndResolve(context.Background(), tok)
	if err == nil {
		t.Fatal("expected error for unregistered org ID, got nil — attacker would be provisioned as default-tenant user")
	}
	if !strings.Contains(err.Error(), "unknown or inactive organisation") {
		t.Errorf("error message = %q, want it to contain %q", err.Error(), "unknown or inactive organisation")
	}
}

// TestValidateAndResolve_OrglessTokenUsesDefaultTenant verifies that a JWT
// with NO urn:zitadel:iam:org:id claim still falls through to DefaultTenantID
// (the legitimate org-less token path must be preserved).
func TestValidateAndResolve_OrglessTokenUsesDefaultTenant(t *testing.T) {
	const issuer = "https://sso.test"
	const audience = "myapp"
	key := genRSAKey(t)
	mw := testMiddlewareWithKeyfunc(t, issuer, audience, key)

	// Pre-seed the user cache so resolveUser does not hit a nil DB pool.
	userID := uuid.New()
	mw.userCache.set(model.DefaultTenantID.String()+":orgless-sub", userID)

	// JWT has no org claim at all.
	claims := validClaims(issuer, audience, "orgless-sub")
	tok := buildJWT(t, key, claims)

	identity, err := mw.validateAndResolve(context.Background(), tok)
	if err != nil {
		t.Fatalf("expected success for org-less token, got: %v", err)
	}
	if identity.TenantID != model.DefaultTenantID {
		t.Errorf("TenantID = %v, want DefaultTenantID", identity.TenantID)
	}
}
