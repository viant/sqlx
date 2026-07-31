package validator

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestQueryContextQueryChunks(t *testing.T) {
	queryCtx := newQueryContext("SELECT id AS Val FROM dept01 WHERE id")
	for i := 1; i <= 5; i++ {
		queryCtx.Append(i, "DeptId", &Path{})
	}

	chunks := queryCtx.QueryChunks(2)

	if assert.Len(t, chunks, 3) {
		assert.Equal(t, "SELECT id AS Val FROM dept01 WHERE id IN (?,?)", chunks[0].Query())
		assert.Equal(t, []interface{}{1, 2}, chunks[0].values)
		assert.Equal(t, "SELECT id AS Val FROM dept01 WHERE id IN (?,?)", chunks[1].Query())
		assert.Equal(t, []interface{}{3, 4}, chunks[1].values)
		assert.Equal(t, "SELECT id AS Val FROM dept01 WHERE id IN (?)", chunks[2].Query())
		assert.Equal(t, []interface{}{5}, chunks[2].values)
	}
}

func TestQueryContextQueryChunksNoopWhenUnderLimit(t *testing.T) {
	queryCtx := newQueryContext("SELECT id AS Val FROM dept01 WHERE id")
	for i := 1; i <= 2; i++ {
		queryCtx.Append(i, "DeptId", &Path{})
	}

	chunks := queryCtx.QueryChunks(2)

	if assert.Len(t, chunks, 1) {
		assert.Same(t, queryCtx, chunks[0])
		assert.Equal(t, "SELECT id AS Val FROM dept01 WHERE id IN (?,?)", chunks[0].Query())
	}
}

func TestInitMaxPlaceholdersFallbackThreshold(t *testing.T) {
	service := New()
	options := NewOptions()

	assert.Equal(t, 0, service.initMaxPlaceholders(nil, options, 994))
	assert.Equal(t, 0, options.MaxPlaceholders)
	assert.Equal(t, 0, service.initMaxPlaceholders(nil, options, 1000))
	assert.Equal(t, 0, options.MaxPlaceholders)
	assert.Equal(t, 1000, service.initMaxPlaceholders(nil, options, 1001))
	assert.Equal(t, 1000, options.MaxPlaceholders)
}

func TestInitMaxPlaceholdersUsesDialectBelowFallback(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if !assert.Nil(t, err) {
		return
	}
	defer db.Close()
	assert.Nil(t, db.PingContext(context.Background()))

	service := New()
	options := NewOptions()

	assert.Equal(t, 994, service.initMaxPlaceholders(db, options, 995))
	assert.Equal(t, 994, options.MaxPlaceholders)
}
