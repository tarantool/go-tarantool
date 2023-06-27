package pool

import (
	"sync"
	"sync/atomic"

	"github.com/tarantool/go-tarantool/v2"
)

type roundRobinStrategy struct {
	conns       []*tarantool.Connection
	indexByAddr map[string]uint
	mutex       sync.RWMutex
	size        uint64
	current     uint64
}

func newRoundRobinStrategy(size int) *roundRobinStrategy {
	return &roundRobinStrategy{
		conns:       make([]*tarantool.Connection, 0, size),
		indexByAddr: make(map[string]uint),
		size:        0,
		current:     0,
	}
}

func (r *roundRobinStrategy) GetConnByAddr(addr string) *tarantool.Connection {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	index, found := r.indexByAddr[addr]
	if !found {
		return nil
	}

	return r.conns[index]
}

func (r *roundRobinStrategy) DeleteConnByAddr(addr string) *tarantool.Connection {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.size == 0 {
		return nil
	}

	index, found := r.indexByAddr[addr]
	if !found {
		return nil
	}

	delete(r.indexByAddr, addr)

	conn := r.conns[index]
	r.conns = append(r.conns[:index], r.conns[index+1:]...)
	r.size -= 1

	for k, v := range r.indexByAddr {
		if v > index {
			r.indexByAddr[k] = v - 1
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

func (r *roundRobinStrategy) GetConnections() []*tarantool.Connection {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	ret := make([]*tarantool.Connection, len(r.conns))
	copy(ret, r.conns)

	return ret
}

func (r *roundRobinStrategy) AddConn(addr string, conn *tarantool.Connection) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if idx, ok := r.indexByAddr[addr]; ok {
		r.conns[idx] = conn
	} else {
		r.conns = append(r.conns, conn)
		r.indexByAddr[addr] = uint(r.size)
		r.size += 1
	}
}

func (r *roundRobinStrategy) nextIndex() uint64 {
	next := atomic.AddUint64(&r.current, 1)
	return (next - 1) % r.size
}
