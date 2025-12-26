package pool

import (
	"context"
	"time"

	"github.com/tarantool/go-tarantool/v3"
)

// TopologyEditor is the interface that must be implemented by a connection pool.
// It describes edit topology methods.
type TopologyEditor interface {
	Add(ctx context.Context, instance Instance) error
	Remove(name string) error
}

// Pooler is the interface that must be implemented by a connection pool.
type Pooler interface {
	TopologyEditor

	ConnectedNow(mode Mode) (bool, error)
	Close() []error
	// CloseGraceful closes connections in the ConnectionPool gracefully. It waits
	// for all requests to complete.
	CloseGraceful() []error
	ConfiguredTimeout(mode Mode) (time.Duration, error)
	NewPrepared(expr string, mode Mode) (*tarantool.Prepared, error)
	NewStream(mode Mode) (*tarantool.Stream, error)
	NewWatcher(key string, callback tarantool.WatchCallback,
		mode Mode) (tarantool.Watcher, error)
	Do(req tarantool.Request, mode Mode) (fut tarantool.Future)
}
