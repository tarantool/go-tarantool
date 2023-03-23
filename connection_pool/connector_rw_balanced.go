package connection_pool

import (
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/balancer"
)

type RWBalancedConnectorAdapter struct {
	pool        Pooler
	defaultMode Mode
}

func NewRWBalancedConnector(pool Pooler, defaultMode Mode) tarantool.Connector {
	return &RWBalancedConnectorAdapter{
		pool:        pool,
		defaultMode: defaultMode,
	}
}

func (b *RWBalancedConnectorAdapter) mode(writeable bool) Mode {
	if writeable {
		return RW
	} else {
		return PreferRO
	}
}
func (b *RWBalancedConnectorAdapter) ConnectedNow() bool {
	ret, err := b.pool.ConnectedNow(b.defaultMode)
	if err != nil {
		return false
	}
	return ret
}

func (b *RWBalancedConnectorAdapter) Close() error {
	return multierror.Append(nil, b.pool.Close()...).ErrorOrNil()
}

func (b *RWBalancedConnectorAdapter) Ping() (besp *tarantool.Response, err error) {
	return b.pool.Ping(b.defaultMode)
}

func (b *RWBalancedConnectorAdapter) ConfiguredTimeout() time.Duration {
	timeout, err := b.pool.ConfiguredTimeout(b.mode(false))
	if err != nil {
		return 0 * time.Second
	}
	return timeout
}

func (b *RWBalancedConnectorAdapter) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}) (besp *tarantool.Response, err error) {
	return b.pool.Select(space, index, offset, limit, iterator, key, b.mode(false))
}

func (b *RWBalancedConnectorAdapter) Insert(space interface{}, tuple interface{}) (besp *tarantool.Response, err error) {
	return b.pool.Insert(space, tuple, b.mode(true))
}

func (b *RWBalancedConnectorAdapter) Replace(space interface{}, tuple interface{}) (besp *tarantool.Response, err error) {
	return b.pool.Replace(space, tuple, b.mode(true))
}

func (b *RWBalancedConnectorAdapter) Delete(space, index interface{}, key interface{}) (besp *tarantool.Response, err error) {
	return b.pool.Delete(space, index, key, b.mode(true))
}

func (b *RWBalancedConnectorAdapter) Update(space, index interface{}, key, ops interface{}) (besp *tarantool.Response, err error) {
	return b.pool.Update(space, index, key, ops, b.mode(true))
}

func (b *RWBalancedConnectorAdapter) Upsert(space interface{}, tuple, ops interface{}) (besp *tarantool.Response, err error) {
	return b.pool.Upsert(space, tuple, ops, b.mode(true))
}

func (b *RWBalancedConnectorAdapter) Call(functionName string, args interface{}) (besp *tarantool.Response, err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(functionName, true)
	return b.pool.Call(script, args, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) Call16(functionName string, args interface{}) (besp *tarantool.Response, err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(functionName, true)
	return b.pool.Call16(script, args, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) Call17(functionName string, args interface{}) (besp *tarantool.Response, err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(functionName, true)
	return b.pool.Call17(script, args, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) Eval(expr string, args interface{}) (besp *tarantool.Response, err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(expr, true)
	return b.pool.Eval(script, args, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) Execute(expr string, args interface{}) (besp *tarantool.Response, err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(expr, true)
	return b.pool.Execute(script, args, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) PrepareExecute(sql string, args map[string]interface{}) (besp *tarantool.Response, err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(sql, true)
	return b.pool.PrepareExecute(script, args, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) GetTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	return b.pool.GetTyped(space, index, key, result, b.mode(false))

}

func (b *RWBalancedConnectorAdapter) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}) (err error) {
	return b.pool.SelectTyped(space, index, offset, limit, iterator, key, result, b.mode(false))

}

func (b *RWBalancedConnectorAdapter) InsertTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return b.pool.InsertTyped(space, tuple, result, b.mode(true))

}

func (b *RWBalancedConnectorAdapter) ReplaceTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return b.pool.ReplaceTyped(space, tuple, result, b.mode(true))

}

func (b *RWBalancedConnectorAdapter) DeleteTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	return b.pool.DeleteTyped(space, index, key, result, b.mode(true))

}

func (b *RWBalancedConnectorAdapter) UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}) (err error) {
	return b.pool.UpdateTyped(space, index, key, ops, result, b.mode(true))

}

func (b *RWBalancedConnectorAdapter) UpsertTyped(space, tuple, ops, result interface{}) (err error) {
	return b.pool.UpsertTyped(space, tuple, ops, result, b.mode(true))

}

func (b *RWBalancedConnectorAdapter) CallTyped(functionName string, args interface{}, result interface{}) (err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(functionName, true)
	return b.pool.CallTyped(script, args, result, b.mode(requiresWrite))

}

func (b *RWBalancedConnectorAdapter) Call16Typed(functionName string, args interface{}, result interface{}) (err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(functionName, true)
	return b.pool.Call16Typed(script, args, result, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) Call17Typed(functionName string, args interface{}, result interface{}) (err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(functionName, true)
	return b.pool.Call17Typed(script, args, result, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) EvalTyped(expr string, args interface{}, result interface{}) (err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(expr, true)
	return b.pool.EvalTyped(script, args, result, b.mode(requiresWrite))

}

func (b *RWBalancedConnectorAdapter) ExecuteTyped(expr string, args interface{}, result interface{}) (tarantool.SQLInfo, []tarantool.ColumnMetaData, error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(expr, true)
	return b.pool.ExecuteTyped(script, args, result, b.mode(requiresWrite))

}

func (b *RWBalancedConnectorAdapter) PrepareExecuteTyped(sql string, args map[string]interface{}, result interface{}) (err error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(sql, true)
	return b.pool.PrepareExecuteTyped(script, args, result, b.mode(requiresWrite))

}

func (b *RWBalancedConnectorAdapter) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}) *tarantool.Future {
	return b.pool.SelectAsync(space, index, offset, limit, iterator, key, b.mode(false))
}

func (b *RWBalancedConnectorAdapter) InsertAsync(space interface{}, tuple interface{}) *tarantool.Future {
	return b.pool.InsertAsync(space, tuple, b.mode(true))
}

func (b *RWBalancedConnectorAdapter) ReplaceAsync(space interface{}, tuple interface{}) *tarantool.Future {
	return b.pool.ReplaceAsync(space, tuple, b.mode(true))
}

func (b *RWBalancedConnectorAdapter) DeleteAsync(space, index interface{}, key interface{}) *tarantool.Future {
	return b.pool.DeleteAsync(space, index, key, b.mode(true))

}

func (b *RWBalancedConnectorAdapter) UpdateAsync(space, index interface{}, key, ops interface{}) *tarantool.Future {
	return b.pool.UpdateAsync(space, index, key, ops, b.mode(true))
}

func (b *RWBalancedConnectorAdapter) UpsertAsync(space interface{}, tuple interface{}, ops interface{}) *tarantool.Future {
	return b.pool.UpsertAsync(space, tuple, ops, b.mode(true))

}

func (b *RWBalancedConnectorAdapter) CallAsync(functionName string, args interface{}) *tarantool.Future {
	script, requiresWrite := balancer.CheckIfRequiresWrite(functionName, true)
	return b.pool.CallAsync(script, args, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) Call16Async(functionName string, args interface{}) *tarantool.Future {
	script, requiresWrite := balancer.CheckIfRequiresWrite(functionName, true)
	return b.pool.Call16Async(script, args, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) Call17Async(functionName string, args interface{}) *tarantool.Future {
	script, requiresWrite := balancer.CheckIfRequiresWrite(functionName, true)
	return b.pool.Call17Async(script, args, b.mode(requiresWrite))
}

func (b *RWBalancedConnectorAdapter) EvalAsync(expr string, args interface{}) *tarantool.Future {
	script, requiresWrite := balancer.CheckIfRequiresWrite(expr, true)
	return b.pool.EvalAsync(script, args, b.mode(requiresWrite))

}

func (b *RWBalancedConnectorAdapter) ExecuteAsync(expr string, args interface{}) *tarantool.Future {
	script, requiresWrite := balancer.CheckIfRequiresWrite(expr, true)
	return b.pool.ExecuteAsync(script, args, b.mode(requiresWrite))

}

func (b *RWBalancedConnectorAdapter) NewPrepared(expr string) (*tarantool.Prepared, error) {
	script, requiresWrite := balancer.CheckIfRequiresWrite(expr, true)
	return b.pool.NewPrepared(script, b.mode(requiresWrite))

}

func (b *RWBalancedConnectorAdapter) NewStream() (*tarantool.Stream, error) {
	return b.pool.NewStream(b.defaultMode)

}

func (b *RWBalancedConnectorAdapter) NewWatcher(key string, callback tarantool.WatchCallback) (tarantool.Watcher, error) {
	return b.pool.NewWatcher(key, callback, b.mode(true))

}

func (b *RWBalancedConnectorAdapter) Do(req tarantool.Request) *tarantool.Future {
	requiresWrite := balancer.CheckIfRequiresWriteForRequest(req, b.defaultMode == RW || b.defaultMode == PreferRW)
	return b.pool.Do(req, b.mode(requiresWrite))

}
