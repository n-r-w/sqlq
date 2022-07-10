package sqlq

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/n-r-w/nerr"
)

// Tx - working with nested transactions
type Tx struct {
	pool *pgxpool.Pool

	ctx     context.Context
	counter int
	tx      pgx.Tx
}

// NewTxNestedPool - create a nested transaction management object
func NewTx(pool *pgxpool.Pool, ctx context.Context) *Tx {
	return &Tx{
		pool:    pool,
		ctx:     ctx,
		counter: 0,
	}
}

// Pool - active connection pool
func (t *Tx) Pool() *pgxpool.Pool {
	return t.pool
}

// Tx - active transaction
func (t *Tx) Tx() pgx.Tx {
	return t.tx
}

// Level - nesting level. 0 - no transaction
func (t *Tx) Level() int {
	return t.counter
}

// Context - active context
func (t *Tx) Context() context.Context {
	return t.ctx
}

// Begin - start a transaction. If the transaction has already started, it is returned
func (t *Tx) Begin() error {
	return t.BeginTx(pgx.ReadCommitted, pgx.ReadWrite)
}

// Begin - start a transaction. If the transaction has already started, it is returned
func (t *Tx) BeginTx(level pgx.TxIsoLevel, mode pgx.TxAccessMode) error {
	if t.counter > 0 {
		t.counter++
		return nil
	}

	tx, err := t.pool.BeginTx(t.ctx, pgx.TxOptions{
		IsoLevel:       level,
		AccessMode:     mode,
		DeferrableMode: "",
	})
	if err != nil {
		return nerr.New(err)
	}

	t.tx = tx
	t.counter++
	return nil
}

// Commit - complete the transaction. If there are nested transactions, the operation is ignored
func (t *Tx) Commit() error {
	if t.counter == 0 {
		return nerr.New("no transaction to commit")
	}

	t.counter--
	if t.counter > 0 {
		return nil
	}

	err := t.tx.Commit(t.ctx)
	t.tx = nil
	return nerr.New(err)
}

// Rollback - roll back the transaction. The counter of nested transactions is reset, because the rollback cannot be partial
func (t *Tx) Rollback() error {
	if t.counter == 0 {
		return nerr.New("no transaction to rollback")
	}

	t.counter = 0
	err := t.tx.Rollback(t.ctx)
	t.tx = nil
	if err != nil {
		return nerr.New(err)
	} else {
		return nil
	}
}
