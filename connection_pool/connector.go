package connection_pool

import (
	"errors"
	"fmt"
	"time"

	"github.com/ice-blockchain/go-tarantool"
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

// Ping sends empty request to Tarantool to check connection.
func (c *ConnectorAdapter) Ping() (*tarantool.Response, error) {
	return c.pool.Ping(c.mode)
}

// ConfiguredTimeout returns a timeout from connections config.
func (c *ConnectorAdapter) ConfiguredTimeout() time.Duration {
	ret, err := c.pool.ConfiguredTimeout(c.mode)
	if err != nil {
		return 0 * time.Second
	}
	return ret
}

// Select performs select to box space.
func (c *ConnectorAdapter) Select(space, index interface{},
	offset, limit, iterator uint32,
	key interface{}) (*tarantool.Response, error) {
	return c.pool.Select(space, index, offset, limit, iterator, key, c.mode)
}

// Insert performs insertion to box space.
func (c *ConnectorAdapter) Insert(space interface{},
	tuple interface{}) (*tarantool.Response, error) {
	return c.pool.Insert(space, tuple, c.mode)
}

// Replace performs "insert or replace" action to box space.
func (c *ConnectorAdapter) Replace(space interface{},
	tuple interface{}) (*tarantool.Response, error) {
	return c.pool.Replace(space, tuple, c.mode)
}

// Delete performs deletion of a tuple by key.
func (c *ConnectorAdapter) Delete(space, index interface{},
	key interface{}) (*tarantool.Response, error) {
	return c.pool.Delete(space, index, key, c.mode)
}

// Update performs update of a tuple by key.
func (c *ConnectorAdapter) Update(space, index interface{},
	key, ops interface{}) (*tarantool.Response, error) {
	return c.pool.Update(space, index, key, ops, c.mode)
}

// Upsert performs "update or insert" action of a tuple by key.
func (c *ConnectorAdapter) Upsert(space interface{},
	tuple, ops interface{}) (*tarantool.Response, error) {
	return c.pool.Upsert(space, tuple, ops, c.mode)
}
func (c *ConnectorAdapter) UpsertTyped(space interface{},
	tuple, ops, result interface{}) error {
	return c.pool.UpsertTyped(space, tuple, ops, result, c.mode)
}

// Call calls registered Tarantool function.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func (c *ConnectorAdapter) Call(functionName string,
	args interface{}) (*tarantool.Response, error) {
	return c.pool.Call(functionName, args, c.mode)
}

// Call16 calls registered Tarantool function.
// It uses request code for Tarantool 1.6, so result is converted to array of arrays
// Deprecated since Tarantool 1.7.2.
func (c *ConnectorAdapter) Call16(functionName string,
	args interface{}) (*tarantool.Response, error) {
	return c.pool.Call16(functionName, args, c.mode)
}

// Call17 calls registered Tarantool function.
// It uses request code for Tarantool >= 1.7, so result is not converted
// (though, keep in mind, result is always array)
func (c *ConnectorAdapter) Call17(functionName string,
	args interface{}) (*tarantool.Response, error) {
	return c.pool.Call17(functionName, args, c.mode)
}

// Eval passes Lua expression for evaluation.
func (c *ConnectorAdapter) Eval(expr string,
	args interface{}) (*tarantool.Response, error) {
	return c.pool.Eval(expr, args, c.mode)
}

// Execute passes sql expression to Tarantool for execution.
func (c *ConnectorAdapter) Execute(expr string,
	args interface{}) (*tarantool.Response, error) {
	return c.pool.Execute(expr, args, c.mode)
}

func (c *ConnectorAdapter) PrepareExecute(sql string, args map[string]interface{}) (resp *tarantool.Response, err error) {
	return c.pool.PrepareExecute(sql, args, c.mode)
}

// GetTyped performs select (with limit = 1 and offset = 0)
// to box space and fills typed result.
func (c *ConnectorAdapter) GetTyped(space, index interface{},
	key interface{}, result interface{}) error {
	return c.pool.GetTyped(space, index, key, result, c.mode)
}

// SelectTyped performs select to box space and fills typed result.
func (c *ConnectorAdapter) SelectTyped(space, index interface{},
	offset, limit, iterator uint32,
	key interface{}, result interface{}) error {
	return c.pool.SelectTyped(space, index, offset, limit, iterator, key, result, c.mode)
}

// InsertTyped performs insertion to box space.
func (c *ConnectorAdapter) InsertTyped(space interface{},
	tuple interface{}, result interface{}) error {
	return c.pool.InsertTyped(space, tuple, result, c.mode)
}

// ReplaceTyped performs "insert or replace" action to box space.
func (c *ConnectorAdapter) ReplaceTyped(space interface{},
	tuple interface{}, result interface{}) error {
	return c.pool.ReplaceTyped(space, tuple, result, c.mode)
}

// DeleteTyped performs deletion of a tuple by key and fills result with deleted tuple.
func (c *ConnectorAdapter) DeleteTyped(space, index interface{},
	key interface{}, result interface{}) error {
	return c.pool.DeleteTyped(space, index, key, result, c.mode)
}

// UpdateTyped performs update of a tuple by key and fills result with updated tuple.
func (c *ConnectorAdapter) UpdateTyped(space, index interface{},
	key, ops interface{}, result interface{}) error {
	return c.pool.UpdateTyped(space, index, key, ops, result, c.mode)
}

// CallTyped calls registered function.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func (c *ConnectorAdapter) CallTyped(functionName string,
	args interface{}, result interface{}) error {
	return c.pool.CallTyped(functionName, args, result, c.mode)
}

// Call16Typed calls registered function.
// It uses request code for Tarantool 1.6, so result is converted to array of arrays
// Deprecated since Tarantool 1.7.2.
func (c *ConnectorAdapter) Call16Typed(functionName string,
	args interface{}, result interface{}) error {
	return c.pool.Call16Typed(functionName, args, result, c.mode)
}

// Call17Typed calls registered function.
// It uses request code for Tarantool >= 1.7, so result is not converted
// (though, keep in mind, result is always array)
func (c *ConnectorAdapter) Call17Typed(functionName string,
	args interface{}, result interface{}) error {
	return c.pool.Call17Typed(functionName, args, result, c.mode)
}

// EvalTyped passes Lua expression for evaluation.
func (c *ConnectorAdapter) EvalTyped(expr string, args interface{},
	result interface{}) error {
	return c.pool.EvalTyped(expr, args, result, c.mode)
}

// ExecuteTyped passes sql expression to Tarantool for execution.
func (c *ConnectorAdapter) ExecuteTyped(expr string, args interface{},
	result interface{}) (tarantool.SQLInfo, []tarantool.ColumnMetaData, error) {
	return c.pool.ExecuteTyped(expr, args, result, c.mode)
}

// SelectAsync sends select request to Tarantool and returns Future.
func (c *ConnectorAdapter) SelectAsync(space, index interface{},
	offset, limit, iterator uint32, key interface{}) *tarantool.Future {
	return c.pool.SelectAsync(space, index, offset, limit, iterator, key, c.mode)
}

// InsertAsync sends insert action to Tarantool and returns Future.
func (c *ConnectorAdapter) InsertAsync(space interface{},
	tuple interface{}) *tarantool.Future {
	return c.pool.InsertAsync(space, tuple, c.mode)
}

// ReplaceAsync sends "insert or replace" action to Tarantool and returns Future.
func (c *ConnectorAdapter) ReplaceAsync(space interface{},
	tuple interface{}) *tarantool.Future {
	return c.pool.ReplaceAsync(space, tuple, c.mode)
}

// DeleteAsync sends deletion action to Tarantool and returns Future.
func (c *ConnectorAdapter) DeleteAsync(space, index interface{},
	key interface{}) *tarantool.Future {
	return c.pool.DeleteAsync(space, index, key, c.mode)
}

// Update sends deletion of a tuple by key and returns Future.
func (c *ConnectorAdapter) UpdateAsync(space, index interface{},
	key, ops interface{}) *tarantool.Future {
	return c.pool.UpdateAsync(space, index, key, ops, c.mode)
}

// UpsertAsync sends "update or insert" action to Tarantool and returns Future.
func (c *ConnectorAdapter) UpsertAsync(space interface{}, tuple interface{},
	ops interface{}) *tarantool.Future {
	return c.pool.UpsertAsync(space, tuple, ops, c.mode)
}

// CallAsync sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func (c *ConnectorAdapter) CallAsync(functionName string,
	args interface{}) *tarantool.Future {
	return c.pool.CallAsync(functionName, args, c.mode)
}

// Call16Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool 1.6, so future's result is always array of arrays.
// Deprecated since Tarantool 1.7.2.
func (c *ConnectorAdapter) Call16Async(functionName string,
	args interface{}) *tarantool.Future {
	return c.pool.Call16Async(functionName, args, c.mode)
}

// Call17Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool >= 1.7, so future's result will not be converted
// (though, keep in mind, result is always array)
func (c *ConnectorAdapter) Call17Async(functionName string,
	args interface{}) *tarantool.Future {
	return c.pool.Call17Async(functionName, args, c.mode)
}

// EvalAsync sends a Lua expression for evaluation and returns Future.
func (c *ConnectorAdapter) EvalAsync(expr string,
	args interface{}) *tarantool.Future {
	return c.pool.EvalAsync(expr, args, c.mode)
}

// ExecuteAsync sends a sql expression for execution and returns Future.
func (c *ConnectorAdapter) ExecuteAsync(expr string,
	args interface{}) *tarantool.Future {
	return c.pool.ExecuteAsync(expr, args, c.mode)
}

// NewPrepared passes a sql statement to Tarantool for preparation
// synchronously.
func (c *ConnectorAdapter) NewPrepared(expr string) (*tarantool.Prepared, error) {
	return c.pool.NewPrepared(expr, c.mode)
}
func (c *ConnectorAdapter) PrepareExecuteTyped(sql string, args map[string]interface{}, result interface{}) (err error) {
	return c.pool.PrepareExecuteTyped(sql, args, result, c.mode)
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
func (c *ConnectorAdapter) Do(req tarantool.Request) *tarantool.Future {
	return c.pool.Do(req, c.mode)
}
