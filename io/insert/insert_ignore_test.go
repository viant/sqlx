package insert

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	mysqlmeta "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
)

func TestInsertIgnoreBuilderMySQLOnly(t *testing.T) {
	mysql := registry.LookupDialect(mysqlmeta.MySQL5())
	builder, err := NewBuilder("links", []string{"report_suite_subscription_id", "campaign_id"}, mysql, "", 3, option.InsertIgnore(true))
	require.NoError(t, err)
	require.Equal(t, "INSERT IGNORE INTO `links`(report_suite_subscription_id,campaign_id) VALUES (?,?),(?,?),(?,?)", builder.Build(nil, option.BatchSize(3)))
	require.Equal(t, "INSERT IGNORE INTO `links`(report_suite_subscription_id,campaign_id) VALUES (?,?)", builder.Build(nil, option.BatchSize(1)))
	for _, name := range []string{"SQLite", "PostgreSQL", ""} {
		_, err = NewBuilder("links", []string{"campaign_id"}, &info.Dialect{Product: database.Product{Name: name}}, "", 1, option.InsertIgnore(true))
		require.ErrorContains(t, err, "INSERT IGNORE requires MySQL")
	}
}

func TestInsertIgnoreServiceConfigurationAndSessionReuse(t *testing.T) {
	type link struct {
		Subscription int `sqlx:"report_suite_subscription_id"`
		Campaign     int `sqlx:"campaign_id"`
	}
	const suffix = "ON DUPLICATE KEY UPDATE campaign_id = VALUES(campaign_id)"
	ctx := context.Background()
	dialect := registry.LookupDialect(mysqlmeta.MySQL5())
	plain, err := New(ctx, nil, "links", dialect)
	require.NoError(t, err)
	ignore, err := New(ctx, nil, "links", dialect, option.InsertIgnore(true))
	require.NoError(t, err)
	suffixed, err := New(ctx, nil, "links", dialect, option.OnDuplicateKeySql(suffix))
	require.NoError(t, err)
	for _, service := range []*Service{plain, ignore, plain, suffixed, ignore, suffixed, plain} {
		// Exercise cold and cached sessions without issuing database operations.
		sess, err := service.newSession(ctx, &link{}, nil, 2, false)
		require.NoError(t, err)
		require.Equal(t, []string{"report_suite_subscription_id", "campaign_id"}, sess.columns.Names())
		require.Empty(t, sess.recordUpdaters, "no supplied identity and no identity allocator")
		prefix := "INSERT INTO "
		if service == ignore {
			prefix = "INSERT IGNORE INTO "
		}
		tail := ""
		if service == suffixed {
			tail = " " + suffix
		}
		require.Equal(t, prefix+"`links`(report_suite_subscription_id,campaign_id) VALUES (?,?),(?,?)"+tail, sess.Builder.Build(nil, option.BatchSize(2), option.OnDuplicateKeySql(sess.OnDuplicateKeySql)))
	}
}
