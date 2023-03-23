package connection_pool

import (
	"sync"

	"github.com/ice-blockchain/go-tarantool"
)

type RoundRobinStrategy struct {
	conns       []*tarantool.Connection
	indexByAddr map[string]uint
	mutex       sync.RWMutex
	size        uint
	current     uint
}

func NewEmptyRoundRobin(size int) *RoundRobinStrategy {
	return &RoundRobinStrategy{
		conns:       make([]*tarantool.Connection, 0, size),
		indexByAddr: make(map[string]uint),
		size:        0,
		current:     0,
	}
}

func (r *RoundRobinStrategy) GetConnByAddr(addr string) *tarantool.Connection {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	index, found := r.indexByAddr[addr]
	if !found {
		return nil
	}

	return r.conns[index]
}

func (r *RoundRobinStrategy) DeleteConnByAddr(addr string) *tarantool.Connection {
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

func (r *RoundRobinStrategy) IsEmpty() bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.size == 0
}

func (r *RoundRobinStrategy) GetNextConnection() *tarantool.Connection {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if r.size == 0 {
		return nil
	}
	return r.conns[r.nextIndex()]
}

func (r *RoundRobinStrategy) GetConnections() []*tarantool.Connection {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	ret := make([]*tarantool.Connection, len(r.conns))
	copy(ret, r.conns)

	return ret
}

func (r *RoundRobinStrategy) AddConn(addr string, conn *tarantool.Connection) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if idx, ok := r.indexByAddr[addr]; ok {
		r.conns[idx] = conn
	} else {
		r.conns = append(r.conns, conn)
		r.indexByAddr[addr] = r.size
		r.size += 1
	}
}

func (r *RoundRobinStrategy) nextIndex() uint {
	ret := r.current % r.size
	r.current++
	return ret
}
