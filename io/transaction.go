package io

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

type Transaction struct {
	*sql.Tx
	Global bool
}

func TransactionFor(ctx context.Context, dialect *info.Dialect, db *sql.DB, options []option.Option) (*Transaction, error) {
	if !dialect.Transactional {
		return nil, nil
	}

	var tx *sql.Tx
	option.Assign(options, &tx)
	if tx != nil {
		return &Transaction{
			Tx:     tx,
			Global: true,
		}, nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		if tx == nil {
			return nil, err
		}

		return nil, (&Transaction{Tx: tx}).RollbackWithErr(err)
	}

	return &Transaction{
		Tx:     tx,
		Global: false,
	}, nil
}

func (t *Transaction) Rollback() error {
	if t.Global {
		return nil
	}

	return t.Tx.Rollback()
}

func (t *Transaction) RollbackWithErr(err error) error {
	if t.Global {
		return err
	}

	if trErr := t.Tx.Rollback(); trErr != nil {
		return fmt.Errorf("failed to rollback: %w, %v", err, trErr)
	}

	return err
}

func (t *Transaction) Commit() error {
	if t.Global {
		return nil
	}

	return t.Tx.Commit()
}
