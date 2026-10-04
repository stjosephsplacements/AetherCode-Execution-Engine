package auth

import (
	"context"

	"github.com/google/uuid"
)

type Identity struct {
	TenantID    uuid.UUID
	UserID      uuid.UUID
	ExternalSub string
	Email       string
	Role        string
}

type contextKey struct{}

func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

func GetIdentity(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(contextKey{}).(Identity)
	return id, ok
}
