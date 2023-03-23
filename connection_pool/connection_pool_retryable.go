package connection_pool

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/ice-blockchain/go-tarantool"
)

type RetryablePool struct {
	pool     *ConnectionPool
	ctx      context.Context
	cancel   context.CancelFunc
	connOpts tarantool.Opts
	opts     OptsPool
}

func NewRetriablePool(ctx context.Context, cancel context.CancelFunc, connPool *ConnectionPool) Pooler {
	return &RetryablePool{
		pool:     connPool,
		ctx:      ctx,
		cancel:   cancel,
		connOpts: connPool.connOpts,
		opts:     connPool.opts,
	}
}
func (rp *RetryablePool) GetConnByMode(userMode ...Mode) (*tarantool.Connection, error) {
	return rp.pool.getConnByMode(ANY, userMode)
}
func (rp *RetryablePool) ConnectedNow(mode Mode) (bool, error) {
	return rp.pool.ConnectedNow(mode)
}

func (rp *RetryablePool) Close() []error {
	rp.cancel()
	return rp.pool.Close()
}

func (rp *RetryablePool) Ping(mode Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return rp.pool.Ping(mode)
	}, mode)
}

func (rp *RetryablePool) ConfiguredTimeout(mode Mode) (time.Duration, error) {
	return rp.pool.ConfiguredTimeout(mode)
}

func (rp *RetryablePool) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}, mode ...Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Select(space, index, offset, limit, iterator, key)
	}, mode...)
}

func (rp *RetryablePool) Insert(space interface{}, tuple interface{}, mode ...Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Insert(space, tuple)
	}, mode...)
}

func (rp *RetryablePool) Replace(space interface{}, tuple interface{}, mode ...Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Replace(space, tuple)
	}, mode...)
}

func (rp *RetryablePool) Delete(space, index interface{}, key interface{}, mode ...Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Delete(space, index, key)
	}, mode...)
}

func (rp *RetryablePool) Update(space, index interface{}, key, ops interface{}, mode ...Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Update(space, index, key, ops)
	}, mode...)
}

func (rp *RetryablePool) Upsert(space interface{}, tuple, ops interface{}, mode ...Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Upsert(space, tuple, ops)
	}, mode...)
}

func (rp *RetryablePool) Call(functionName string, args interface{}, mode Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Call(functionName, args)
	}, mode)
}

func (rp *RetryablePool) Call16(functionName string, args interface{}, mode Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Call16(functionName, args)
	}, mode)
}

func (rp *RetryablePool) Call17(functionName string, args interface{}, mode Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Call17(functionName, args)
	}, mode)
}

func (rp *RetryablePool) Eval(expr string, args interface{}, mode Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Eval(expr, args)
	}, mode)
}

func (rp *RetryablePool) Execute(expr string, args interface{}, mode Mode) (*tarantool.Response, error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Execute(expr, args)
	}, mode)
}

func (rp *RetryablePool) PrepareExecute(sql string, args map[string]interface{}, mode Mode) (resp *tarantool.Response, err error) {
	return retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.PrepareExecute(sql, args)
	}, mode)
}

func (rp *RetryablePool) GetTyped(space, index interface{}, key interface{}, result interface{}, mode ...Mode) error {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.GetTyped(space, index, key, result)
	}, mode...)
}

func (rp *RetryablePool) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}, mode ...Mode) error {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.SelectTyped(space, index, offset, limit, iterator, key, result)
	}, mode...)
}

func (rp *RetryablePool) InsertTyped(space interface{}, tuple interface{}, result interface{}, mode ...Mode) error {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.InsertTyped(space, tuple, result)
	}, mode...)
}

func (rp *RetryablePool) ReplaceTyped(space interface{}, tuple interface{}, result interface{}, mode ...Mode) error {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.ReplaceTyped(space, tuple, result)
	}, mode...)
}

func (rp *RetryablePool) DeleteTyped(space, index interface{}, key interface{}, result interface{}, mode ...Mode) error {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.DeleteTyped(space, index, key, result)
	}, mode...)
}

func (rp *RetryablePool) UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}, mode ...Mode) error {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.UpdateTyped(space, index, key, ops, result)
	}, mode...)
}

func (rp *RetryablePool) UpsertTyped(space, tuple, ops, result interface{}, mode ...Mode) (err error) {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.UpsertTyped(space, tuple, ops, result)
	}, mode...)
}

func (rp *RetryablePool) CallTyped(functionName string, args interface{}, result interface{}, mode Mode) error {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.CallTyped(functionName, args, result)
	}, mode)
}

func (rp *RetryablePool) Call16Typed(functionName string, args interface{}, result interface{}, mode Mode) error {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.Call16Typed(functionName, args, result)
	}, mode)
}

func (rp *RetryablePool) Call17Typed(functionName string, args interface{}, result interface{}, mode Mode) error {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.Call17Typed(functionName, args, result)
	}, mode)
}

func (rp *RetryablePool) EvalTyped(expr string, args interface{}, result interface{}, mode Mode) error {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.EvalTyped(expr, args, result)
	}, mode)
}

func (rp *RetryablePool) ExecuteTyped(expr string, args interface{}, result interface{}, mode Mode) (sqlInfo tarantool.SQLInfo, col []tarantool.ColumnMetaData, err error) {
	err = retryTyped(rp, func(conn *tarantool.Connection) error {
		sqlInfo, col, err = conn.ExecuteTyped(expr, args, result)
		return err
	}, mode)
	return sqlInfo, col, err
}

func (rp *RetryablePool) PrepareExecuteTyped(sql string, args map[string]interface{}, result interface{}, mode Mode) (err error) {
	return retryTyped(rp, func(conn *tarantool.Connection) error {
		return conn.PrepareExecuteTyped(sql, args, result)
	}, mode)
}

func (rp *RetryablePool) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}, mode ...Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.SelectAsync(space, index, offset, limit, iterator, key)
	}, mode...)
}

func (rp *RetryablePool) InsertAsync(space interface{}, tuple interface{}, mode ...Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.InsertAsync(space, tuple)
	}, mode...)
}

func (rp *RetryablePool) ReplaceAsync(space interface{}, tuple interface{}, mode ...Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.ReplaceAsync(space, tuple)
	}, mode...)
}

func (rp *RetryablePool) DeleteAsync(space, index interface{}, key interface{}, mode ...Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.DeleteAsync(space, index, key)
	}, mode...)
}

func (rp *RetryablePool) UpdateAsync(space, index interface{}, key, ops interface{}, mode ...Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.UpdateAsync(space, index, key, ops)
	}, mode...)
}

func (rp *RetryablePool) UpsertAsync(space interface{}, tuple interface{}, ops interface{}, mode ...Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.UpsertAsync(space, tuple, ops)
	}, mode...)
}

func (rp *RetryablePool) CallAsync(functionName string, args interface{}, mode Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.CallAsync(functionName, args, mode)
	}, mode)
}

func (rp *RetryablePool) Call16Async(functionName string, args interface{}, mode Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.Call16Async(functionName, args, mode)
	}, mode)
}

func (rp *RetryablePool) Call17Async(functionName string, args interface{}, mode Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.Call17Async(functionName, args, mode)
	}, mode)
}

func (rp *RetryablePool) EvalAsync(expr string, args interface{}, mode Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.EvalAsync(expr, args, mode)
	}, mode)
}

func (rp *RetryablePool) ExecuteAsync(expr string, args interface{}, mode Mode) *tarantool.Future {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.ExecuteAsync(expr, args, mode)
	}, mode)
}

func (rp *RetryablePool) NewPrepared(expr string, mode Mode) (*tarantool.Prepared, error) {
	return retry[*tarantool.Prepared](rp, func(conn *tarantool.Connection) (*tarantool.Prepared, error) {
		return conn.NewPrepared(expr)
	}, mode)
}

func (rp *RetryablePool) NewStream(mode Mode) (*tarantool.Stream, error) {
	return retry[*tarantool.Stream](rp, func(conn *tarantool.Connection) (*tarantool.Stream, error) {
		return conn.NewStream()
	}, mode)
}

func (rp *RetryablePool) NewWatcher(key string, callback tarantool.WatchCallback, mode Mode) (tarantool.Watcher, error) {
	return retry[tarantool.Watcher](rp, func(conn *tarantool.Connection) (tarantool.Watcher, error) {
		return conn.NewWatcher(key, callback)
	}, mode)
}

func (rp *RetryablePool) Do(req tarantool.Request, mode Mode) (fut *tarantool.Future) {
	return retryAsync(rp, func(conn *tarantool.Connection) *tarantool.Future {
		return rp.pool.Do(req, mode)
	}, mode)
}

func retryTyped(rp *RetryablePool, f func(conn *tarantool.Connection) error, mode ...Mode) (err error) {
	_, err = retry[*tarantool.Response](rp, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return nil, f(conn)
	}, mode...)

	return
}

func retryAsync(rp *RetryablePool, impl func(conn *tarantool.Connection) *tarantool.Future, mode ...Mode) (fut *tarantool.Future) {
	fut = tarantool.NewFuture()
	if rp.ctx.Err() != nil {
		fut.SetError(rp.ctx.Err())
		return
	}
	c, err := rp.pool.getConnByMode(ANY, mode)
	if err != nil {
		fut.SetError(err)
		return
	}
	if c == nil {
		fut.SetError(ErrNoConnection)
		return
	}
	fut = impl(c)
	resp := make(chan *tarantool.Future, 1)
	go func() {
		if fut.Err() != nil && rp.shouldRetry(fut.Err()) {
			ctx, cancel := context.WithTimeout(rp.ctx, rp.connOpts.Timeout)
			defer cancel()
			fut = tarantool.NewFuture()
			fut.SetError(backoff.RetryNotify(
				func() error {
					if ctx.Err() != nil {
						return backoff.Permanent(ctx.Err())
					}
					c, err = rp.pool.getConnByMode(ANY, mode)
					if err != nil {
						return backoff.Permanent(err)
					}
					if c == nil {
						return backoff.Permanent(ErrNoConnection)
					}
					if fut = impl(c); fut.Err() != nil {
						if rp.shouldRetry(err) {
							return err
						} else {
							return backoff.Permanent(err)
						}
					}
					return nil
				},
				rp.backoff(ctx),
				func(e error, next time.Duration) {
					log.Printf("Call failed with %v. Retrying...", e)
				},
			))
			resp <- fut
		}
	}()
	return
}

func retry[T any](rp *RetryablePool, impl func(conn *tarantool.Connection) (T, error), mode ...Mode) (r T, err error) {

	if rp.ctx.Err() != nil {
		return r, rp.ctx.Err()
	}
	c, err := rp.pool.getConnByMode(ANY, mode)
	if err != nil {
		return r, err
	}
	if c == nil {
		return r, ErrNoConnection
	}
	if r, err = impl(c); err != nil && rp.shouldRetry(err) {
		ctx, cancel := context.WithTimeout(rp.ctx, rp.connOpts.Timeout)
		defer cancel()
		err = backoff.RetryNotify(
			func() error {
				if ctx.Err() != nil {
					err = ctx.Err()
					return backoff.Permanent(err)
				}
				c, err = rp.pool.getConnByMode(ANY, mode)
				if err != nil {
					return backoff.Permanent(err)
				}
				if c == nil {
					err = ErrNoConnection
					return backoff.Permanent(err)
				}
				if r, err = impl(c); err != nil {
					if rp.shouldRetry(err) {
						return err
					} else {
						return backoff.Permanent(err)
					}
				}
				return nil
			},
			rp.backoff(ctx),
			func(e error, next time.Duration) {
				log.Printf("Call failed with %v. Retrying...", e)
			},
		)
	}
	return
}

func (rp *RetryablePool) backoff(ctx context.Context) backoff.BackOffContext {
	return backoff.WithContext(&backoff.ExponentialBackOff{
		InitialInterval:     time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          5,
		MaxInterval:         time.Second,
		MaxElapsedTime:      rp.connOpts.Timeout,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}, ctx)
}

func (connMulti *RetryablePool) shouldRetry(err error) (shouldRetry bool) {
	switch err.(type) {
	case tarantool.ClientError:
		code := err.(tarantool.ClientError).Code
		if code == tarantool.ErrConnectionNotReady ||
			code == tarantool.ErrConnectionShutdown ||
			code == tarantool.ErrConnectionClosed {
			shouldRetry = true
		}
		return
	case tarantool.Error:
		code := err.(tarantool.Error).Code
		msg := err.(tarantool.Error).Msg
		if code == tarantool.ER_NONMASTER ||
			code == tarantool.ER_READONLY ||
			code == tarantool.ER_TUPLE_FORMAT_LIMIT ||
			code == tarantool.ER_UNKNOWN ||
			code == tarantool.ER_MEMORY_ISSUE ||
			code == tarantool.ER_UNKNOWN_REPLICA ||
			code == tarantool.ER_REPLICASET_UUID_MISMATCH ||
			code == tarantool.ER_REPLICASET_UUID_IS_RO ||
			code == tarantool.ER_REPLICA_ID_IS_RESERVED ||
			code == tarantool.ER_REPLICA_MAX ||
			code == tarantool.ER_INVALID_XLOG ||
			code == tarantool.ER_NO_CONNECTION ||
			code == tarantool.ER_ACTIVE_TRANSACTION ||
			code == tarantool.ER_SESSION_CLOSED ||
			code == tarantool.ER_TRANSACTION_CONFLICT ||
			code == tarantool.ER_MEMTX_MAX_TUPLE_SIZE ||
			code == tarantool.ER_VIEW_IS_RO ||
			code == tarantool.ER_NO_TRANSACTION ||
			code == tarantool.ER_SYSTEM ||
			code == tarantool.ER_LOADING ||
			code == tarantool.ER_LOCAL_INSTANCE_ID_IS_READ_ONLY ||
			code == tarantool.ER_BACKUP_IN_PROGRESS ||
			code == tarantool.ER_READ_VIEW_ABORTED ||
			code == tarantool.ER_CASCADE_ROLLBACK ||
			code == tarantool.ER_VY_QUOTA_TIMEOUT ||
			code == tarantool.ER_TRANSACTION_YIELD ||
			code == tarantool.ER_BOOTSTRAP_READONLY ||
			code == tarantool.ER_REPLICA_NOT_ANON ||
			code == tarantool.ER_CANNOT_REGISTER ||
			code == tarantool.ER_UNCOMMITTED_FOREIGN_SYNC_TXNS ||
			code == tarantool.ER_SYNC_MASTER_MISMATCH ||
			code == tarantool.ER_SYNC_QUORUM_TIMEOUT ||
			code == tarantool.ER_SYNC_ROLLBACK ||
			code == tarantool.ER_QUORUM_WAIT ||
			code == tarantool.ER_TOO_EARLY_SUBSCRIBE ||
			code == tarantool.ER_INTERFERING_PROMOTE ||
			code == tarantool.ER_ELECTION_DISABLED ||
			code == tarantool.ER_TXN_ROLLBACK ||
			code == tarantool.ER_NOT_LEADER ||
			code == tarantool.ER_SYNC_QUEUE_UNCLAIMED ||
			code == tarantool.ER_SYNC_QUEUE_FOREIGN ||
			code == tarantool.ER_WAL_IO ||
			strings.Contains(msg, "read-only") {
			shouldRetry = true
		}
		return
	}
	return
}
