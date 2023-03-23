package connection_pool

import (
	"sync"

	"github.com/ice-blockchain/go-tarantool"
)

// watcherContainer is a very simple implementation of a thread-safe container
// for watchers. It is not expected that there will be too many watchers and
// they will registered/unregistered too frequently.
//
// Otherwise, the implementation will need to be optimized.
type watcherContainer struct {
	head  *poolWatcher
	mutex sync.RWMutex
}

// add adds a watcher to the container.
func (c *watcherContainer) add(watcher *poolWatcher) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	watcher.next = c.head
	c.head = watcher
}

// remove removes a watcher from the container.
func (c *watcherContainer) remove(watcher *poolWatcher) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if watcher == c.head {
		c.head = watcher.next
		return true
	} else if c.head != nil {
		cur := c.head
		for cur.next != nil {
			if cur.next == watcher {
				cur.next = watcher.next
				return true
			}
			cur = cur.next
		}
	}
	return false
}

// foreach iterates over the container to the end or until the call returns
// false.
func (c *watcherContainer) foreach(call func(watcher *poolWatcher) error) error {
	cur := c.head
	for cur != nil {
		if err := call(cur); err != nil {
			return err
		}
		cur = cur.next
	}
	return nil
}

// poolWatcher is an internal implementation of the tarantool.Watcher interface.
type poolWatcher struct {
	// The watcher container data. We can split the structure into two parts
	// in the future: a watcher data and a watcher container data, but it looks
	// simple at now.

	// next item in the watcher container.
	next *poolWatcher
	// container is the container for all active poolWatcher objects.
	container *watcherContainer

	// The watcher data.
	// mode of the watcher.
	mode     Mode
	key      string
	callback tarantool.WatchCallback
	// watchers is a map connection -> connection watcher.
	watchers map[string]tarantool.Watcher
	// unregistered is true if the watcher already unregistered.
	unregistered bool
	// mutex for the pool watcher.
	mutex sync.Mutex
}

// Unregister unregisters the pool watcher.
func (w *poolWatcher) Unregister() {
	w.mutex.Lock()
	unregistered := w.unregistered
	w.mutex.Unlock()

	if !unregistered && w.container.remove(w) {
		w.mutex.Lock()
		w.unregistered = true
		for _, watcher := range w.watchers {
			watcher.Unregister()
		}
		w.mutex.Unlock()
	}
}

// watch adds a watcher for the connection.
func (w *poolWatcher) watch(conn *tarantool.Connection) error {
	addr := conn.Addr()

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.unregistered {
		if _, ok := w.watchers[addr]; ok {
			return nil
		}

		if watcher, err := conn.NewWatcher(w.key, w.callback); err == nil {
			w.watchers[addr] = watcher
			return nil
		} else {
			return err
		}
	}
	return nil
}

// unwatch removes a watcher for the connection.
func (w *poolWatcher) unwatch(conn *tarantool.Connection) {
	addr := conn.Addr()

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.unregistered {
		if watcher, ok := w.watchers[addr]; ok {
			watcher.Unregister()
			delete(w.watchers, addr)
		}
	}
}
