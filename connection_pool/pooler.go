package connection_pool

import (
	"time"

	"github.com/ice-blockchain/go-tarantool"
)

// Pooler is the interface that must be implemented by a connection pool.
type Pooler interface {
	ConnectedNow(mode Mode) (bool, error)
	Close() []error
	Ping(mode Mode) (*tarantool.Response, error)
	ConfiguredTimeout(mode Mode) (time.Duration, error)

	Select(space, index interface{}, offset, limit, iterator uint32,
		key interface{}, mode ...Mode) (*tarantool.Response, error)
	Insert(space interface{}, tuple interface{},
		mode ...Mode) (*tarantool.Response, error)
	Replace(space interface{}, tuple interface{},
		mode ...Mode) (*tarantool.Response, error)
	Delete(space, index interface{}, key interface{},
		mode ...Mode) (*tarantool.Response, error)
	Update(space, index interface{}, key, ops interface{},
		mode ...Mode) (*tarantool.Response, error)
	Upsert(space interface{}, tuple, ops interface{},
		mode ...Mode) (*tarantool.Response, error)
	Call(functionName string, args interface{},
		mode Mode) (*tarantool.Response, error)
	Call16(functionName string, args interface{},
		mode Mode) (*tarantool.Response, error)
	Call17(functionName string, args interface{},
		mode Mode) (*tarantool.Response, error)
	Eval(expr string, args interface{},
		mode Mode) (*tarantool.Response, error)
	Execute(expr string, args interface{},
		mode Mode) (*tarantool.Response, error)
	PrepareExecute(sql string, args map[string]interface{}, mode Mode) (resp *tarantool.Response, err error)

	GetTyped(space, index interface{}, key interface{}, result interface{},
		mode ...Mode) error
	SelectTyped(space, index interface{}, offset, limit, iterator uint32,
		key interface{}, result interface{}, mode ...Mode) error
	InsertTyped(space interface{}, tuple interface{}, result interface{},
		mode ...Mode) error
	ReplaceTyped(space interface{}, tuple interface{}, result interface{},
		mode ...Mode) error
	DeleteTyped(space, index interface{}, key interface{}, result interface{},
		mode ...Mode) error
	UpdateTyped(space, index interface{}, key, ops interface{},
		result interface{}, mode ...Mode) error
	UpsertTyped(space, tuple, ops, result interface{}, mode ...Mode) (err error)
	CallTyped(functionName string, args interface{}, result interface{},
		mode Mode) error
	Call16Typed(functionName string, args interface{}, result interface{},
		mode Mode) error
	Call17Typed(functionName string, args interface{}, result interface{},
		mode Mode) error
	EvalTyped(expr string, args interface{}, result interface{},
		mode Mode) error
	ExecuteTyped(expr string, args interface{}, result interface{},
		mode Mode) (tarantool.SQLInfo, []tarantool.ColumnMetaData, error)
	PrepareExecuteTyped(sql string, args map[string]interface{}, result interface{},
		mode Mode) (err error)

	SelectAsync(space, index interface{}, offset, limit, iterator uint32,
		key interface{}, mode ...Mode) *tarantool.Future
	InsertAsync(space interface{}, tuple interface{},
		mode ...Mode) *tarantool.Future
	ReplaceAsync(space interface{}, tuple interface{},
		mode ...Mode) *tarantool.Future
	DeleteAsync(space, index interface{}, key interface{},
		mode ...Mode) *tarantool.Future
	UpdateAsync(space, index interface{}, key, ops interface{},
		mode ...Mode) *tarantool.Future
	UpsertAsync(space interface{}, tuple interface{}, ops interface{},
		mode ...Mode) *tarantool.Future
	CallAsync(functionName string, args interface{},
		mode Mode) *tarantool.Future
	Call16Async(functionName string, args interface{},
		mode Mode) *tarantool.Future
	Call17Async(functionName string, args interface{},
		mode Mode) *tarantool.Future
	EvalAsync(expr string, args interface{},
		mode Mode) *tarantool.Future
	ExecuteAsync(expr string, args interface{},
		mode Mode) *tarantool.Future

	NewPrepared(expr string, mode Mode) (*tarantool.Prepared, error)
	NewStream(mode Mode) (*tarantool.Stream, error)
	NewWatcher(key string, callback tarantool.WatchCallback,
		mode Mode) (tarantool.Watcher, error)

	Do(req tarantool.Request, mode Mode) (fut *tarantool.Future)
}
