package update_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/errx"
	"github.com/viant/sqlx/io/update"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestService_Exec_ClassifiesConstraintErrors(t *testing.T) {
	type entity struct {
		ID       int     `sqlx:"name=id,primaryKey=true"`
		Code     string  `sqlx:"name=code"`
		Required *string `sqlx:"name=required"`
	}

	ctx := context.Background()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "update-errors.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.Exec(`CREATE TABLE update_errors (
id INTEGER PRIMARY KEY,
code TEXT NOT NULL UNIQUE,
required TEXT NOT NULL
)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO update_errors (id, code, required) VALUES
(1, 'existing', 'value'),
(2, 'updatable', 'value')`)
	require.NoError(t, err)

	service, err := update.New(ctx, db, "update_errors")
	require.NoError(t, err)
	required := "value"

	t.Run("duplicate key", func(t *testing.T) {
		_, actualErr := service.Exec(ctx, &entity{ID: 2, Code: "existing", Required: &required})
		require.Error(t, actualErr)
		require.True(t, errors.Is(actualErr, errx.ErrDuplicateKey))
		require.False(t, errors.Is(actualErr, errx.ErrConstraint))

		var detail *errx.Error
		require.ErrorAs(t, actualErr, &detail)
		require.Equal(t, "update", detail.Op)
		require.Equal(t, "update_errors", detail.Table)
		require.Error(t, detail.Cause)
	})

	t.Run("not null constraint", func(t *testing.T) {
		_, actualErr := service.Exec(ctx, &entity{ID: 2, Code: "updatable", Required: nil})
		require.Error(t, actualErr)
		require.True(t, errors.Is(actualErr, errx.ErrConstraint))
		require.False(t, errors.Is(actualErr, errx.ErrDuplicateKey))

		var detail *errx.Error
		require.ErrorAs(t, actualErr, &detail)
		require.Equal(t, "update", detail.Op)
		require.Equal(t, "update_errors", detail.Table)
		require.Error(t, detail.Cause)
	})
}
