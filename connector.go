package tarantool

import "time"

// Doer is an interface that performs requests asynchronously.
type Doer interface {
	// Do performs a request asynchronously.
	Do(req Request) Future
}

type Connector interface {
	Doer
	ConnectedNow() bool
	Close() error
	ConfiguredTimeout() time.Duration
	NewPrepared(expr string) (*Prepared, error)
	NewStream() (*Stream, error)
	NewWatcher(key string, callback WatchCallback) (Watcher, error)
}
