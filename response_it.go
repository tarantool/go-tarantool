package tarantool

import (
	"time"
)

// ResponseIterator is an interface for iteration over a set of responses.
//
// Deprecated: the method will be removed in the next major version,
// use Connector.NewWatcher() instead of box.session.push().
type ResponseIterator interface {
	// Next tries to switch to a next Response and returns true if it exists.
	Next() bool
	// Value returns a current Response if it exists, nil otherwise.
	Value() *Response
	// Err returns error if it happens.
	Err() error
}

// TimeoutResponseIterator is an interface that extends ResponseIterator
// and adds the ability to change a timeout for the Next() call.
//
// Deprecated: the method will be removed in the next major version,
// use Connector.NewWatcher() instead of box.session.push().
type TimeoutResponseIterator interface {
	ResponseIterator
	// WithTimeout allows to set up a timeout for the Next() call.
	// Note: in the current implementation, there is a timeout for each
	// response (the timeout for the request is reset by each push message):
	// Connection's Opts.Timeout. You need to increase the value if necessary.
	WithTimeout(timeout time.Duration) TimeoutResponseIterator
}
