package mysql_test

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox"
	"testing"
)

func Test_Sequence(t *testing.T) {

	db, err := sql.Open("mysql", "root:dev@tcp(localhost:3307)/ci_ads")
	if err != nil {
		t.Error(err)
		return
	}
	srv := metadata.New()

	seq := &sink.Sequence{}
	err = srv.Info(context.Background(), db, info.KindSequences, seq, option.NewArgs("", "ci_ads", "CI_AD_ORDER"))
	assert.Nil(t, err)

	toolbox.Dump(seq)
}
