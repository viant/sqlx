package insert_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/option"
	"github.com/viant/sqlx/testutil/sqlfault"
)

func TestColdInsertSessionRetainsSuppliedProduct(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		constructor, product, cached bool
	}{
		{name: "constructor dialect", constructor: true},
		{name: "invocation dialect"},
		{name: "constructor product", constructor: true, product: true},
		{name: "invocation product", product: true},
		{name: "cached session", constructor: true, cached: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(name TEXT)")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			resolved, err := config.Dialect(ctx, h.DB)
			if err != nil {
				t.Fatal(err)
			}
			db := h.FaultDB(t, func(_ context.Context, c sqlfault.Call) error {
				if strings.Contains(c.SQL, "sqlite_version()") {
					return errors.New("supplied product must not be redetected")
				}
				return nil
			})
			db.SetMaxOpenConns(1)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var provided option.Option = resolved
			if tc.product {
				provided = &resolved.Product
			}
			var constructor, invocation []option.Option
			if tc.constructor {
				constructor = append(constructor, provided)
			} else {
				invocation = append(invocation, provided)
			}
			if tc.cached {
				constructor = append(constructor, option.MetaSessionCacheKey("records"), option.MetaSessionCache{Map: &sync.Map{}})
			}
			service, err := insert.New(ctx, db, "records", constructor...)
			if err != nil {
				t.Fatal(err)
			}
			// Metadata must retain the transaction owner and exclude operation Args.
			invocation = append(invocation, tx, option.NewArgs("not a metadata criterion"))
			_, err = service.NewSession(ctx, &struct{ Name string }{Name: "value"}, db, 1, invocation...)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
