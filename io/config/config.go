package config

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

//Config represents general config
type Config struct {
	TableName string
	TagName   string
	Identity  string
	Columns   io.Columns
	Dialect   *info.Dialect
	Mapper    io.ColumnMapper
	Builder   io.Builder
}

//New creates a  config
func New(tableName string) *Config {
	return &Config{TableName: tableName}
}

//ApplyOption applied config option
func (c *Config) ApplyOption(ctx context.Context, db *sql.DB, options ...option.Option) error {
	for _, opt := range options {
		switch actual := opt.(type) {
		case *info.Dialect:
			c.Dialect = actual
		case option.Tag:
			c.TagName = string(actual)
		case io.Columns:
			c.Columns = actual
		case option.Identity:
			c.Identity = string(actual)
		default:
			if mapper, ok := opt.(io.ColumnMapper); ok {
				c.Mapper = mapper
				continue
			}
			if builder, ok := opt.(io.Builder); ok {
				c.Builder = builder
				continue
			}
		}
	}
	c.ensureTagName()
	c.ensureMapper()
	return c.ensureDialect(ctx, db)
}

func (c *Config) ensureMapper() {
	if c.Mapper == nil {
		c.Mapper = io.StructColumnMapper
	}
}

func (c *Config) ensureDialect(ctx context.Context, db *sql.DB) error {
	if c.Dialect != nil {
		return nil
	}
	dialect, err := Dialect(ctx, db)
	if err != nil {
		return err
	}
	c.Dialect = dialect
	return nil
}

func (c *Config) ensureTagName() {
	if c.TagName == "" {
		c.TagName = option.TagSqlx
	}
}
