package pool

import (
	"time"

	"github.com/tarantool/go-tarantool/v2"
)

// Pooler is the interface that must be implemented by a connection pool.
type Pooler interface {
	ConnectedNow(mode Mode) (bool, error)
	Close() []error
	ConfiguredTimeout(mode Mode) (time.Duration, error)
	NewPrepared(expr string, mode Mode) (*tarantool.Prepared, error)
	NewStream(mode Mode) (*tarantool.Stream, error)
	NewWatcher(key string, callback tarantool.WatchCallback,
		mode Mode) (tarantool.Watcher, error)
	Do(req tarantool.Request, mode Mode) (fut *tarantool.Future)

	// Deprecated: the method will be removed in the next major version,
	// use a PingRequest object + Do() instead.
	Ping(mode Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a SelectRequest object + Do() instead.
	Select(space, index interface{}, offset, limit uint32, iterator tarantool.Iter,
		key interface{}, mode ...Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use an InsertRequest object + Do() instead.
	Insert(space interface{}, tuple interface{},
		mode ...Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a ReplaceRequest object + Do() instead.
	Replace(space interface{}, tuple interface{},
		mode ...Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a DeleteRequest object + Do() instead.
	Delete(space, index interface{}, key interface{},
		mode ...Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a UpdateRequest object + Do() instead.
	Update(space, index interface{}, key interface{}, ops *tarantool.Operations,
		mode ...Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a UpsertRequest object + Do() instead.
	Upsert(space interface{}, tuple interface{}, ops *tarantool.Operations,
		mode ...Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a CallRequest object + Do() instead.
	Call(functionName string, args interface{},
		mode Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a Call16Request object + Do() instead.
	Call16(functionName string, args interface{},
		mode Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a Call17Request object + Do() instead.
	Call17(functionName string, args interface{},
		mode Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use an EvalRequest object + Do() instead.
	Eval(expr string, args interface{},
		mode Mode) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use an ExecuteRequest object + Do() instead.
	Execute(expr string, args interface{},
		mode Mode) ([]interface{}, error)

	// Deprecated: the method will be removed in the next major version,
	// use a SelectRequest object + Do() instead.
	GetTyped(space, index interface{}, key interface{}, result interface{},
		mode ...Mode) error
	// Deprecated: the method will be removed in the next major version,
	// use a SelectRequest object + Do() instead.
	SelectTyped(space, index interface{}, offset, limit uint32, iterator tarantool.Iter,
		key interface{}, result interface{}, mode ...Mode) error
	// Deprecated: the method will be removed in the next major version,
	// use an InsertRequest object + Do() instead.
	InsertTyped(space interface{}, tuple interface{}, result interface{},
		mode ...Mode) error
	// Deprecated: the method will be removed in the next major version,
	// use a ReplaceRequest object + Do() instead.
	ReplaceTyped(space interface{}, tuple interface{}, result interface{},
		mode ...Mode) error
	// Deprecated: the method will be removed in the next major version,
	// use a DeleteRequest object + Do() instead.
	DeleteTyped(space, index interface{}, key interface{}, result interface{},
		mode ...Mode) error
	// Deprecated: the method will be removed in the next major version,
	// use a UpdateRequest object + Do() instead.
	UpdateTyped(space, index interface{}, key interface{},
		ops *tarantool.Operations, result interface{}, mode ...Mode) error
	// Deprecated: the method will be removed in the next major version,
	// use a CallRequest object + Do() instead.
	CallTyped(functionName string, args interface{}, result interface{},
		mode Mode) error
	// Deprecated: the method will be removed in the next major version,
	// use a Call16Request object + Do() instead.
	Call16Typed(functionName string, args interface{}, result interface{},
		mode Mode) error
	// Deprecated: the method will be removed in the next major version,
	// use a Call17Request object + Do() instead.
	Call17Typed(functionName string, args interface{}, result interface{},
		mode Mode) error
	// Deprecated: the method will be removed in the next major version,
	// use an EvalRequest object + Do() instead.
	EvalTyped(expr string, args interface{}, result interface{},
		mode Mode) error
	// Deprecated: the method will be removed in the next major version,
	// use an ExecuteRequest object + Do() instead.
	ExecuteTyped(expr string, args interface{}, result interface{},
		mode Mode) (tarantool.SQLInfo, []tarantool.ColumnMetaData, error)

	// Deprecated: the method will be removed in the next major version,
	// use a SelectRequest object + Do() instead.
	SelectAsync(space, index interface{}, offset, limit uint32, iterator tarantool.Iter,
		key interface{}, mode ...Mode) *tarantool.Future
	// Deprecated: the method will be removed in the next major version,
	// use an InsertRequest object + Do() instead.
	InsertAsync(space interface{}, tuple interface{},
		mode ...Mode) *tarantool.Future
	// Deprecated: the method will be removed in the next major version,
	// use a ReplaceRequest object + Do() instead.
	ReplaceAsync(space interface{}, tuple interface{},
		mode ...Mode) *tarantool.Future
	// Deprecated: the method will be removed in the next major version,
	// use a DeleteRequest object + Do() instead.
	DeleteAsync(space, index interface{}, key interface{},
		mode ...Mode) *tarantool.Future
	// Deprecated: the method will be removed in the next major version,
	// use a UpdateRequest object + Do() instead.
	UpdateAsync(space, index interface{}, key interface{},
		ops *tarantool.Operations, mode ...Mode) *tarantool.Future
	// Deprecated: the method will be removed in the next major version,
	// use a UpsertRequest object + Do() instead.
	UpsertAsync(space interface{}, tuple interface{}, ops *tarantool.Operations,
		mode ...Mode) *tarantool.Future
	// Deprecated: the method will be removed in the next major version,
	// use a CallRequest object + Do() instead.
	CallAsync(functionName string, args interface{},
		mode Mode) *tarantool.Future
	// Deprecated: the method will be removed in the next major version,
	// use a Call16Request object + Do() instead.
	Call16Async(functionName string, args interface{},
		mode Mode) *tarantool.Future
	// Deprecated: the method will be removed in the next major version,
	// use a Call17Request object + Do() instead.
	Call17Async(functionName string, args interface{},
		mode Mode) *tarantool.Future
	// Deprecated: the method will be removed in the next major version,
	// use an EvalRequest object + Do() instead.
	EvalAsync(expr string, args interface{},
		mode Mode) *tarantool.Future
	// Deprecated: the method will be removed in the next major version,
	// use an ExecuteRequest object + Do() instead.
	ExecuteAsync(expr string, args interface{},
		mode Mode) *tarantool.Future
}
