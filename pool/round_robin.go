package pool

import (
	"sync"
	"sync/atomic"

	"github.com/tarantool/go-tarantool/v2"
)

type roundRobinStrategy struct {
	conns     []*tarantool.Connection
	indexById map[string]uint
	mutex     sync.RWMutex
	size      uint64
	current   uint64
}

func newRoundRobinStrategy(size int) *roundRobinStrategy {
	return &roundRobinStrategy{
		conns:     make([]*tarantool.Connection, 0, size),
		indexById: make(map[string]uint, size),
		size:      0,
		current:   0,
	}
}

func (r *roundRobinStrategy) GetConnection(id string) *tarantool.Connection {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	index, found := r.indexById[id]
	if !found {
		return nil
	}

	return r.conns[index]
}

func (r *roundRobinStrategy) DeleteConnection(id string) *tarantool.Connection {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.size == 0 {
		return nil
	}

	index, found := r.indexById[id]
	if !found {
		return nil
	}

	delete(r.indexById, id)

	conn := r.conns[index]
	r.conns = append(r.conns[:index], r.conns[index+1:]...)
	r.size -= 1

	for k, v := range r.indexById {
		if v > index {
			r.indexById[k] = v - 1
		}
	}

	return conn
}

func (r *roundRobinStrategy) IsEmpty() bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.size == 0
}

func (r *roundRobinStrategy) GetNextConnection() *tarantool.Connection {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if r.size == 0 {
		return nil
	}
	return r.conns[r.nextIndex()]
}

func (r *roundRobinStrategy) GetConnections() map[string]*tarantool.Connection {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	conns := map[string]*tarantool.Connection{}
	for id, index := range r.indexById {
		conns[id] = r.conns[index]
	}

	return conns
}

func (r *roundRobinStrategy) AddConnection(id string, conn *tarantool.Connection) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if idx, ok := r.indexById[id]; ok {
		r.conns[idx] = conn
	} else {
		r.conns = append(r.conns, conn)
		r.indexById[id] = uint(r.size)
		r.size += 1
	}
}

func (r *roundRobinStrategy) nextIndex() uint64 {
	next := atomic.AddUint64(&r.current, 1)
	return (next - 1) % r.size
}
