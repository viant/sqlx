package read_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/testutil/sqlfault"
)

func TestSourceErrorProvenance(t *testing.T) {
	for _, mode := range []string{"prepare", "query", "next", "callback", "cancel", "recovered", "exhausted", "reconnect", "recovered callback", "cache", "opt out"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2)")
			cause := errors.New("private SQL credential sentinel")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				phase := mode
				switch mode {
				case "cancel", "recovered", "exhausted", "reconnect", "recovered callback", "opt out":
					phase = "prepare"
				}
				if call.Phase != phase {
					return nil
				}
				calls++
				if (mode == "recovered" || mode == "recovered callback") && calls > 1 {
					return nil
				}
				if mode == "cancel" {
					cancel()
				}
				return cause
			})
			opts := []read.Option{}
			if mode != "opt out" {
				opts = append(opts, read.WithSourceErrorProvenance())
			}
			if mode == "cache" {
				opts = append(opts, read.WithCache(retryCacheFailure{}))
			}
			if mode == "recovered" || mode == "exhausted" || mode == "reconnect" || mode == "recovered callback" {
				opts = append(opts, read.WithRetry(read.RetryPolicy{Attempts: 2, Recoverable: func(err error) bool {
					var marker *read.SourceError
					require.False(t, errors.As(err, &marker), "retry predicate must retain original input")
					return true
				}, Reconnect: func(context.Context) (*sql.DB, error) {
					if mode == "reconnect" {
						return nil, errors.New("reconnect failed")
					}
					return db, nil
				}}))
			}
			reader, err := read.New(ctx, db, "SELECT id FROM records", func() any { return &lookupRow{} }, opts...)
			require.NoError(t, err)
			err = reader.QueryAll(ctx, func(any) error {
				if mode == "callback" || mode == "recovered callback" {
					return cause
				}
				return nil
			})
			if reader.Stmt() != nil {
				defer reader.Stmt().Close()
			}
			var marker *read.SourceError
			marked := errors.As(err, &marker)
			want := mode == "prepare" || mode == "query" || mode == "exhausted"
			require.Equal(t, want, marked)
			if want {
				require.ErrorIs(t, err, cause)
				require.Equal(t, marker.Unwrap().Error(), marker.Error())
				stage := "prepare"
				if mode == "query" {
					stage = "query"
				}
				require.Equal(t, stage, marker.Stage())
			}
			if mode == "recovered" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
