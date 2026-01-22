package pool

import (
	"errors"
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool/v3"
)

// ConnectorAdapter allows to use Pooler as Connector.
type ConnectorAdapter struct {
	pool Pooler
	mode Mode
}

var _ tarantool.Connector = (*ConnectorAdapter)(nil)

// NewConnectorAdapter creates a new ConnectorAdapter object for a pool
// and with a mode. All requests to the pool will be executed in the
// specified mode.
func NewConnectorAdapter(pool Pooler, mode Mode) *ConnectorAdapter {
	return &ConnectorAdapter{pool: pool, mode: mode}
}

// ConnectedNow reports if connections is established at the moment.
func (c *ConnectorAdapter) ConnectedNow() bool {
	ret, err := c.pool.ConnectedNow(c.mode)
	if err != nil {
		return false
	}
	return ret
}

// ClosedNow reports if the connector is closed by user or all connections
// in the specified mode closed.
func (c *ConnectorAdapter) Close() error {
	errs := c.pool.Close()
	if len(errs) == 0 {
		return nil
	}

	err := errors.New("failed to close connection pool")
	for _, e := range errs {
		err = fmt.Errorf("%s: %w", err.Error(), e)
	}
	return err
}

// ConfiguredTimeout returns a timeout from connections config.
func (c *ConnectorAdapter) ConfiguredTimeout() time.Duration {
	ret, err := c.pool.ConfiguredTimeout(c.mode)
	if err != nil {
		return 0 * time.Second
	}
	return ret
}

// NewPrepared passes a sql statement to Tarantool for preparation
// synchronously.
func (c *ConnectorAdapter) NewPrepared(expr string) (*tarantool.Prepared, error) {
	return c.pool.NewPrepared(expr, c.mode)
}

// NewStream creates new Stream object for connection.
//
// Since v. 2.10.0, Tarantool supports streams and interactive transactions over
// them. To use interactive transactions, memtx_use_mvcc_engine box option
// should be set to true.
// Since 1.7.0
func (c *ConnectorAdapter) NewStream() (*tarantool.Stream, error) {
	return c.pool.NewStream(c.mode)
}

// NewWatcher creates new Watcher object for the pool
//
// Since 1.10.0
func (c *ConnectorAdapter) NewWatcher(key string,
	callback tarantool.WatchCallback) (tarantool.Watcher, error) {
	return c.pool.NewWatcher(key, callback, c.mode)
}

// Do performs a request asynchronously on the connection.
func (c *ConnectorAdapter) Do(req tarantool.Request) tarantool.Future {
	return c.pool.Do(req, c.mode)
}
