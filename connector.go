package tarantool

import "time"

type Connector interface {
	ConnectedNow() bool
	Close() error
	Ping() (resp *Response, err error)
	ConfiguredTimeout() time.Duration

	Select(space, index interface{}, offset, limit, iterator uint32, key interface{}) (resp *Response, err error)
	Insert(space interface{}, tuple interface{}) (resp *Response, err error)
	Replace(space interface{}, tuple interface{}) (resp *Response, err error)
	Delete(space, index interface{}, key interface{}) (resp *Response, err error)
	Update(space, index interface{}, key, ops interface{}) (resp *Response, err error)
	Upsert(space interface{}, tuple, ops interface{}) (resp *Response, err error)
	Call(functionName string, args interface{}) (resp *Response, err error)
	Call16(functionName string, args interface{}) (resp *Response, err error)
	Call17(functionName string, args interface{}) (resp *Response, err error)
	Eval(expr string, args interface{}) (resp *Response, err error)
	Execute(expr string, args interface{}) (resp *Response, err error)
	PrepareExecute(sql string, args map[string]interface{}) (resp *Response, err error)

	GetTyped(space, index interface{}, key interface{}, result interface{}) (err error)
	SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}) (err error)
	InsertTyped(space interface{}, tuple interface{}, result interface{}) (err error)
	ReplaceTyped(space interface{}, tuple interface{}, result interface{}) (err error)
	DeleteTyped(space, index interface{}, key interface{}, result interface{}) (err error)
	UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}) (err error)
	UpsertTyped(space, tuple, ops, result interface{}) (err error)
	CallTyped(functionName string, args interface{}, result interface{}) (err error)
	Call16Typed(functionName string, args interface{}, result interface{}) (err error)
	Call17Typed(functionName string, args interface{}, result interface{}) (err error)
	EvalTyped(expr string, args interface{}, result interface{}) (err error)
	ExecuteTyped(expr string, args interface{}, result interface{}) (SQLInfo, []ColumnMetaData, error)
	PrepareExecuteTyped(sql string, args map[string]interface{}, result interface{}) (err error)

	SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}) *Future
	InsertAsync(space interface{}, tuple interface{}) *Future
	ReplaceAsync(space interface{}, tuple interface{}) *Future
	DeleteAsync(space, index interface{}, key interface{}) *Future
	UpdateAsync(space, index interface{}, key, ops interface{}) *Future
	UpsertAsync(space interface{}, tuple interface{}, ops interface{}) *Future
	CallAsync(functionName string, args interface{}) *Future
	Call16Async(functionName string, args interface{}) *Future
	Call17Async(functionName string, args interface{}) *Future
	EvalAsync(expr string, args interface{}) *Future
	ExecuteAsync(expr string, args interface{}) *Future

	NewPrepared(expr string) (*Prepared, error)
	NewStream() (*Stream, error)
	NewWatcher(key string, callback WatchCallback) (Watcher, error)

	Do(req Request) (fut *Future)
}
