package config_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/config"
)

func TestDialectPreservesCancellation(t *testing.T) {
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	held, err := h.DB.Conn(context.Background())
	require.NoError(t, err)
	defer held.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = config.Dialect(ctx, h.DB)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	_, err = config.Dialect(canceled, h.DB)
	require.ErrorIs(t, err, context.Canceled)
}
