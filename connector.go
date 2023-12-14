package tarantool

import "time"

type Connector interface {
	ConnectedNow() bool
	Close() error
	ConfiguredTimeout() time.Duration
	NewPrepared(expr string) (*Prepared, error)
	NewStream() (*Stream, error)
	NewWatcher(key string, callback WatchCallback) (Watcher, error)
	Do(req Request) (fut *Future)

	// Deprecated: the method will be removed in the next major version,
	// use a PingRequest object + Do() instead.
	Ping() ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a SelectRequest object + Do() instead.
	Select(space, index interface{}, offset, limit uint32, iterator Iter,
		key interface{}) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use an InsertRequest object + Do() instead.
	Insert(space interface{}, tuple interface{}) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a ReplicaRequest object + Do() instead.
	Replace(space interface{}, tuple interface{}) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a DeleteRequest object + Do() instead.
	Delete(space, index interface{}, key interface{}) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a UpdateRequest object + Do() instead.
	Update(space, index interface{}, key interface{}, ops *Operations) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a UpsertRequest object + Do() instead.
	Upsert(space interface{}, tuple interface{}, ops *Operations) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a CallRequest object + Do() instead.
	Call(functionName string, args interface{}) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a Call16Request object + Do() instead.
	Call16(functionName string, args interface{}) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use a Call17Request object + Do() instead.
	Call17(functionName string, args interface{}) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use an EvalRequest object + Do() instead.
	Eval(expr string, args interface{}) ([]interface{}, error)
	// Deprecated: the method will be removed in the next major version,
	// use an ExecuteRequest object + Do() instead.
	Execute(expr string, args interface{}) ([]interface{}, error)

	// Deprecated: the method will be removed in the next major version,
	// use a SelectRequest object + Do() instead.
	GetTyped(space, index interface{}, key interface{}, result interface{}) error
	// Deprecated: the method will be removed in the next major version,
	// use a SelectRequest object + Do() instead.
	SelectTyped(space, index interface{}, offset, limit uint32, iterator Iter, key interface{},
		result interface{}) error
	// Deprecated: the method will be removed in the next major version,
	// use an InsertRequest object + Do() instead.
	InsertTyped(space interface{}, tuple interface{}, result interface{}) error
	// Deprecated: the method will be removed in the next major version,
	// use a ReplaceRequest object + Do() instead.
	ReplaceTyped(space interface{}, tuple interface{}, result interface{}) error
	// Deprecated: the method will be removed in the next major version,
	// use a DeleteRequest object + Do() instead.
	DeleteTyped(space, index interface{}, key interface{}, result interface{}) error
	// Deprecated: the method will be removed in the next major version,
	// use a UpdateRequest object + Do() instead.
	UpdateTyped(space, index interface{}, key interface{}, ops *Operations,
		result interface{}) error
	// Deprecated: the method will be removed in the next major version,
	// use a CallRequest object + Do() instead.
	CallTyped(functionName string, args interface{}, result interface{}) error
	// Deprecated: the method will be removed in the next major version,
	// use a Call16Request object + Do() instead.
	Call16Typed(functionName string, args interface{}, result interface{}) error
	// Deprecated: the method will be removed in the next major version,
	// use a Call17Request object + Do() instead.
	Call17Typed(functionName string, args interface{}, result interface{}) error
	// Deprecated: the method will be removed in the next major version,
	// use an EvalRequest object + Do() instead.
	EvalTyped(expr string, args interface{}, result interface{}) error
	// Deprecated: the method will be removed in the next major version,
	// use an ExecuteRequest object + Do() instead.
	ExecuteTyped(expr string, args interface{},
		result interface{}) (SQLInfo, []ColumnMetaData, error)

	// Deprecated: the method will be removed in the next major version,
	// use a SelectRequest object + Do() instead.
	SelectAsync(space, index interface{}, offset, limit uint32, iterator Iter,
		key interface{}) *Future
	// Deprecated: the method will be removed in the next major version,
	// use an InsertRequest object + Do() instead.
	InsertAsync(space interface{}, tuple interface{}) *Future
	// Deprecated: the method will be removed in the next major version,
	// use a ReplaceRequest object + Do() instead.
	ReplaceAsync(space interface{}, tuple interface{}) *Future
	// Deprecated: the method will be removed in the next major version,
	// use a DeleteRequest object + Do() instead.
	DeleteAsync(space, index interface{}, key interface{}) *Future
	// Deprecated: the method will be removed in the next major version,
	// use a UpdateRequest object + Do() instead.
	UpdateAsync(space, index interface{}, key interface{}, ops *Operations) *Future
	// Deprecated: the method will be removed in the next major version,
	// use a UpsertRequest object + Do() instead.
	UpsertAsync(space interface{}, tuple interface{}, ops *Operations) *Future
	// Deprecated: the method will be removed in the next major version,
	// use a CallRequest object + Do() instead.
	CallAsync(functionName string, args interface{}) *Future
	// Deprecated: the method will be removed in the next major version,
	// use a Call16Request object + Do() instead.
	Call16Async(functionName string, args interface{}) *Future
	// Deprecated: the method will be removed in the next major version,
	// use a Call17Request object + Do() instead.
	Call17Async(functionName string, args interface{}) *Future
	// Deprecated: the method will be removed in the next major version,
	// use an EvalRequest object + Do() instead.
	EvalAsync(expr string, args interface{}) *Future
	// Deprecated: the method will be removed in the next major version,
	// use an ExecuteRequest object + Do() instead.
	ExecuteAsync(expr string, args interface{}) *Future
}
