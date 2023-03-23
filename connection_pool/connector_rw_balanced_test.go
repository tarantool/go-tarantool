package connection_pool_test

import (
	"errors"
	"github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/balancer"
	"github.com/ice-blockchain/go-tarantool/connection_pool"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

type requiresWriteMockPool struct {
	requiresWrite bool
	called        uint64
}

func (r *requiresWriteMockPool) validateMode(mode connection_pool.Mode) error {
	atomic.AddUint64(&r.called, 1)
	if mode == connection_pool.ANY {
		return errors.New("Any connection is not applicable for RW balanced pool, it shold be RO or RW")
	}
	if r.requiresWrite && (mode == connection_pool.RO || mode == connection_pool.PreferRO) {
		return errors.New("read only connection requested for writeable case")
	}
	if !r.requiresWrite && (mode == connection_pool.RW || mode == connection_pool.PreferRW) {
		return errors.New("writable connection requested for read only case")
	}
	return nil
}
func (r *requiresWriteMockPool) ConnectedNow(mode connection_pool.Mode) (bool, error) {
	return true, r.validateMode(mode)
}

func (r *requiresWriteMockPool) Close() []error {
	return []error{}
}

func (r *requiresWriteMockPool) Ping(mode connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) ConfiguredTimeout(mode connection_pool.Mode) (time.Duration, error) {
	return time.Millisecond, r.validateMode(mode)
}

func (r *requiresWriteMockPool) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}, mode ...connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) Insert(space interface{}, tuple interface{}, mode ...connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) Replace(space interface{}, tuple interface{}, mode ...connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) Delete(space, index interface{}, key interface{}, mode ...connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) Update(space, index interface{}, key, ops interface{}, mode ...connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) Upsert(space interface{}, tuple, ops interface{}, mode ...connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) Call(functionName string, args interface{}, mode connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) Call16(functionName string, args interface{}, mode connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) Call17(functionName string, args interface{}, mode connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) Eval(expr string, args interface{}, mode connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) Execute(expr string, args interface{}, mode connection_pool.Mode) (*tarantool.Response, error) {
	return nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) PrepareExecute(sql string, args map[string]interface{}, mode connection_pool.Mode) (resp *tarantool.Response, err error) {
	return nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) GetTyped(space, index interface{}, key interface{}, result interface{}, mode ...connection_pool.Mode) error {
	return r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}, mode ...connection_pool.Mode) error {
	return r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) InsertTyped(space interface{}, tuple interface{}, result interface{}, mode ...connection_pool.Mode) error {
	return r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) ReplaceTyped(space interface{}, tuple interface{}, result interface{}, mode ...connection_pool.Mode) error {
	return r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) DeleteTyped(space, index interface{}, key interface{}, result interface{}, mode ...connection_pool.Mode) error {
	return r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}, mode ...connection_pool.Mode) error {
	return r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) UpsertTyped(space, tuple, ops, result interface{}, mode ...connection_pool.Mode) (err error) {
	return r.validateMode(mode[0])
}

func (r *requiresWriteMockPool) CallTyped(functionName string, args interface{}, result interface{}, mode connection_pool.Mode) error {
	return r.validateMode(mode)
}

func (r *requiresWriteMockPool) Call16Typed(functionName string, args interface{}, result interface{}, mode connection_pool.Mode) error {
	return r.validateMode(mode)
}

func (r *requiresWriteMockPool) Call17Typed(functionName string, args interface{}, result interface{}, mode connection_pool.Mode) error {
	return r.validateMode(mode)
}

func (r *requiresWriteMockPool) EvalTyped(expr string, args interface{}, result interface{}, mode connection_pool.Mode) error {
	return r.validateMode(mode)
}

func (r *requiresWriteMockPool) ExecuteTyped(expr string, args interface{}, result interface{}, mode connection_pool.Mode) (tarantool.SQLInfo, []tarantool.ColumnMetaData, error) {
	return tarantool.SQLInfo{}, nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) PrepareExecuteTyped(sql string, args map[string]interface{}, result interface{}, mode connection_pool.Mode) (err error) {
	return r.validateMode(mode)
}

func (r *requiresWriteMockPool) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}, mode ...connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode[0]))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) InsertAsync(space interface{}, tuple interface{}, mode ...connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode[0]))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) ReplaceAsync(space interface{}, tuple interface{}, mode ...connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode[0]))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) DeleteAsync(space, index interface{}, key interface{}, mode ...connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode[0]))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) UpdateAsync(space, index interface{}, key, ops interface{}, mode ...connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode[0]))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) UpsertAsync(space interface{}, tuple interface{}, ops interface{}, mode ...connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode[0]))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) CallAsync(functionName string, args interface{}, mode connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) Call16Async(functionName string, args interface{}, mode connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) Call17Async(functionName string, args interface{}, mode connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) EvalAsync(expr string, args interface{}, mode connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) ExecuteAsync(expr string, args interface{}, mode connection_pool.Mode) *tarantool.Future {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode))
	f.AppendPush(nil)
	return f
}

func (r *requiresWriteMockPool) NewPrepared(expr string, mode connection_pool.Mode) (*tarantool.Prepared, error) {
	return nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) NewStream(mode connection_pool.Mode) (*tarantool.Stream, error) {
	return nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) NewWatcher(key string, callback tarantool.WatchCallback, mode connection_pool.Mode) (tarantool.Watcher, error) {
	return nil, r.validateMode(mode)
}

func (r *requiresWriteMockPool) Do(req tarantool.Request, mode connection_pool.Mode) (fut *tarantool.Future) {
	f := tarantool.NewFuture()
	f.SetError(r.validateMode(mode))
	f.AppendPush(nil)
	return f
}

func NewRWConnectorAdapter(pool connection_pool.Pooler, mode connection_pool.Mode) *connection_pool.RWBalancedConnectorAdapter {
	return connection_pool.NewRWBalancedConnector(pool, mode).(*connection_pool.RWBalancedConnectorAdapter)
}

func TestRWConnectorConnectedNow(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, connection_pool.RW)

	require.Falsef(t, c.ConnectedNow(), "unexpected result")
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorGetTyped(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.GetTyped(reqSpace, reqIndex, reqKey, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorSelect(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Select(reqSpace, reqIndex, reqOffset, reqLimit, reqIterator, reqKey)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorSelectTyped(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.SelectTyped(reqSpace, reqIndex, reqOffset, reqLimit,
		reqIterator, reqKey, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorSelectAsync(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.SelectAsync(reqSpace, reqIndex, reqOffset, reqLimit,
		reqIterator, reqKey)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorInsert(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Insert(reqSpace, reqTuple)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorInsertTyped(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.InsertTyped(reqSpace, reqTuple, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorInsertAsync(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.InsertAsync(reqSpace, reqTuple)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorReplace(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Replace(reqSpace, reqTuple)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorReplaceTyped(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.ReplaceTyped(reqSpace, reqTuple, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorReplaceAsync(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.ReplaceAsync(reqSpace, reqTuple)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorDelete(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Delete(reqSpace, reqIndex, reqKey)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorDeleteTyped(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.DeleteTyped(reqSpace, reqIndex, reqKey, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorDeleteAsync(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.DeleteAsync(reqSpace, reqIndex, reqKey)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorUpdate(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Update(reqSpace, reqIndex, reqKey, reqOps)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorUpdateTyped(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.UpdateTyped(reqSpace, reqIndex, reqKey, reqOps, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorUpdateAsync(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.UpdateAsync(reqSpace, reqIndex, reqKey, reqOps)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorUpsert(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Upsert(reqSpace, reqTuple, reqOps)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorUpsertAsync(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.UpsertAsync(reqSpace, reqTuple, reqOps)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCall(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Call(reqFunctionName, reqArgs)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCallTyped(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.CallTyped(reqFunctionName, reqArgs, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorCallAsync(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.CallAsync(reqFunctionName, reqArgs)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCallNonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Call("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCallTypedNonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.CallTyped("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorCallAsyncNonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.CallAsync("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCall16(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Call16(reqFunctionName, reqArgs)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCall16Typed(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.Call16Typed(reqFunctionName, reqArgs, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}
func TestRWConnectorCall16Async(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.Call16Async(reqFunctionName, reqArgs)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCall16NonWritable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Call16("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCall16TypedNonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.Call16Typed("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}
func TestRWConnectorCall16AsyncNonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.Call16Async("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCall17(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Call17(reqFunctionName, reqArgs)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorCall17Typed(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)
	_, err := c.Call17(reqFunctionName, reqArgs)
	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCall17Async(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.Call17Async(reqFunctionName, reqArgs)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}
func TestRWConnectorCall17NonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Call17("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorCall17TypedNonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.Call17Typed("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorCall17AsyncNonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.Call17Async("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorEval(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Eval(reqFunctionName, reqArgs)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorEvalTyped(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.EvalTyped(reqFunctionName, reqArgs, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorEvalAsync(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.EvalAsync(reqFunctionName, reqArgs)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorEvalNonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Eval("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorEvalTypedNonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	err := c.EvalTyped("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorEvalAsyncNonWriteable(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: false}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.EvalAsync("{{"+string(balancer.NonWritable)+"}}"+reqFunctionName, reqArgs)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")

}

func TestRWConnectorExecute(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.Execute(reqFunctionName, reqArgs)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorExecuteTyped(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, _, err := c.ExecuteTyped(reqFunctionName, reqArgs, reqResult)

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorExecuteAsync(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.ExecuteAsync(reqFunctionName, reqArgs)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorNewPrepared(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.NewPrepared(reqFunctionName)
	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorNewStream(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	_, err := c.NewStream()

	require.NoError(t, err)
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}

func TestRWConnectorDo(t *testing.T) {
	m := &requiresWriteMockPool{requiresWrite: true}
	c := NewRWConnectorAdapter(m, testMode)

	fut := c.Do(reqRequest)

	require.NoError(t, fut.Err())
	require.Equalf(t, uint64(1), m.called, "should be called only once")
}
