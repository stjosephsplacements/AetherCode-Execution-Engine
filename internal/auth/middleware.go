package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

type Middleware struct {
	jwks        keyfunc.Keyfunc
	jwksCancel  context.CancelFunc
	issuer      string
	audience    string
	pool        *pgxpool.Pool
	tenantCache *ttlCache[uuid.UUID]
	userCache   *ttlCache[uuid.UUID]
}

// NewMiddleware creates an OIDC JWT validation middleware.
// jwksURL should point to the internal JWKS endpoint (e.g. http://127.0.0.1:8087/oauth/v2/keys).
// The issuer hostname is injected as the Host header for internal requests to Zitadel.
func NewMiddleware(issuer, jwksURL, audience string, pool *pgxpool.Pool) (*Middleware, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Extract host from issuer URL to inject as Host header on internal JWKS requests.
	// Zitadel requires the correct Host header to route to the right instance.
	issuerHost := issuer
	if strings.HasPrefix(issuerHost, "https://") {
		issuerHost = issuerHost[len("https://"):]
	} else if strings.HasPrefix(issuerHost, "http://") {
		issuerHost = issuerHost[len("http://"):]
	}

	client := &http.Client{
		Transport: &hostInjectTransport{
			host: issuerHost,
			base: http.DefaultTransport,
		},
	}

	k, err := keyfunc.NewDefaultOverrideCtx(ctx, []string{jwksURL}, keyfunc.Override{
		Client: client,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("auth: failed to create JWKS keyfunc: %w", err)
	}

	tc, uc := initCaches(ctx)
	return &Middleware{
		jwks:        k,
		jwksCancel:  cancel,
		issuer:      issuer,
		audience:    audience,
		pool:        pool,
		tenantCache: tc,
		userCache:   uc,
	}, nil
}

type hostInjectTransport struct {
	host string
	base http.RoundTripper
}

func (t *hostInjectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Host = t.host
	return t.base.RoundTrip(req)
}

// Wrap returns an http.Handler that validates JWT tokens when present.
// It does NOT reject missing tokens — route handlers decide that per their auth policy.
func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := extractBearerToken(r)
		if token == "" {
			next.ServeHTTP(w, r)
			return
		}

		identity, err := m.validateAndResolve(r.Context(), token)
		if err != nil {
			slog.Debug("auth: invalid token", "err", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid or expired token"}) //nolint:errcheck // best-effort: write the error body
			return
		}

		ctx := WithIdentity(r.Context(), *identity)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (m *Middleware) validateAndResolve(ctx context.Context, tokenStr string) (*Identity, error) {
	opts := []jwt.ParserOption{
		jwt.WithIssuer(m.issuer),
		jwt.WithAudience(m.audience), // always enforced
		jwt.WithExpirationRequired(),
	}

	token, err := jwt.Parse(tokenStr, m.jwks.KeyfuncCtx(ctx), opts...)
	if err != nil {
		return nil, fmt.Errorf("jwt parse: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type")
	}

	sub, _ := claims.GetSubject()
	if sub == "" {
		return nil, fmt.Errorf("missing sub claim")
	}

	email, _ := claims["email"].(string)
	name := claimString(claims, "name")
	if name == "" {
		name = claimString(claims, "preferred_username")
	}

	// Resolve Zitadel org to tenant. The org ID claim varies by Zitadel config.
	tenantID := model.DefaultTenantID
	if orgID := claimString(claims, "urn:zitadel:iam:org:id"); orgID != "" {
		if tid, err := m.resolveTenant(ctx, orgID); err == nil {
			tenantID = tid
		}
	}

	userID, err := m.resolveUser(ctx, tenantID, sub, email, name)
	if err != nil {
		return nil, fmt.Errorf("resolve user: %w", err)
	}

	role := "student"
	if roles, ok := claims["urn:zitadel:iam:org:project:roles"].(map[string]interface{}); ok {
		if _, isAdmin := roles["admin"]; isAdmin {
			role = "admin"
		} else if _, isFac := roles["faculty"]; isFac {
			role = "faculty"
		}
	}

	return &Identity{
		TenantID:    tenantID,
		UserID:      userID,
		ExternalSub: sub,
		Email:       email,
		Role:        role,
	}, nil
}

func (m *Middleware) resolveTenant(ctx context.Context, orgID string) (uuid.UUID, error) {
	if id, ok := m.tenantCache.get(orgID); ok {
		return id, nil
	}
	var id uuid.UUID
	err := m.pool.QueryRow(ctx,
		`SELECT id FROM tenants WHERE slug = $1 AND active = true`, orgID).Scan(&id)
	if err != nil {
		return uuid.Nil, err
	}
	m.tenantCache.set(orgID, id)
	return id, nil
}

func (m *Middleware) resolveUser(ctx context.Context, tenantID uuid.UUID, sub, email, name string) (uuid.UUID, error) {
	cacheKey := tenantID.String() + ":" + sub
	if id, ok := m.userCache.get(cacheKey); ok {
		return id, nil
	}

	// Try lookup first (fast path)
	var id uuid.UUID
	err := m.pool.QueryRow(ctx,
		`SELECT id FROM users WHERE tenant_id = $1 AND external_subject_id = $2`,
		tenantID, sub).Scan(&id)
	if err == nil {
		m.userCache.set(cacheKey, id)
		return id, nil
	}

	// Auto-provision
	err = m.pool.QueryRow(ctx,
		`INSERT INTO users (tenant_id, external_subject_id, email, display_name)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (tenant_id, external_subject_id) DO UPDATE SET email = EXCLUDED.email
		 RETURNING id`,
		tenantID, sub, email, name).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("auto-provision user: %w", err)
	}
	m.userCache.set(cacheKey, id)
	slog.Info("user auto-provisioned", "user_id", id, "sub", sub)
	return id, nil
}

func extractBearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return parts[1]
}

func claimString(claims jwt.MapClaims, key string) string {
	v, _ := claims[key].(string)
	return v
}

// Close shuts down the background JWKS refresh goroutine.
func (m *Middleware) Close() {
	m.jwksCancel()
}
