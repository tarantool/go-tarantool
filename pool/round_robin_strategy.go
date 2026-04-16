package pool

import (
	"sync"
	"sync/atomic"

	"github.com/tarantool/go-tarantool/v3"
)

// roundRobinStrategy implements a round-robin connection selection strategy.
// All connections are active and selected in round-robin order.
type roundRobinStrategy struct {
	conns     []*tarantool.Connection
	indexById map[string]uint
	mutex     sync.RWMutex
	current   uint64
}

// newRoundRobinStrategy creates a new round-robin strategy.
func newRoundRobinStrategy(expectedSize int) *roundRobinStrategy {
	return &roundRobinStrategy{
		conns:     make([]*tarantool.Connection, 0, expectedSize),
		indexById: make(map[string]uint, expectedSize),
		current:   0,
	}
}

// Add adds or updates a connection with the given ID (upsert).
func (s *roundRobinStrategy) Add(id string, conn *tarantool.Connection) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if idx, exists := s.indexById[id]; exists {
		s.conns[idx] = conn // Update existing
		return
	}

	s.indexById[id] = uint(len(s.conns))
	s.conns = append(s.conns, conn)
}

// Get returns a connection by ID.
func (s *roundRobinStrategy) Get(id string) *tarantool.Connection {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	index, exists := s.indexById[id]
	if !exists {
		return nil
	}

	return s.conns[index]
}

// Remove removes a connection by ID.
func (s *roundRobinStrategy) Remove(id string) *tarantool.Connection {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	index, exists := s.indexById[id]
	if !exists {
		return nil
	}

	delete(s.indexById, id)

	conn := s.conns[index]
	s.conns = append(s.conns[:index], s.conns[index+1:]...)

	for id, idx := range s.indexById {
		if idx > index {
			s.indexById[id] = idx - 1
		}
	}

	return conn
}

// Next returns the next connection in round-robin order.
func (s *roundRobinStrategy) Next() *tarantool.Connection {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if len(s.conns) == 0 {
		return nil
	}

	return s.conns[s.nextIndex()]
}

// Connections returns a map of all connections by their ID.
func (s *roundRobinStrategy) Connections() map[string]*tarantool.Connection {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	result := make(map[string]*tarantool.Connection, len(s.indexById))
	for id, index := range s.indexById {
		result[id] = s.conns[index]
	}
	return result
}

// Len returns the number of connections managed by this strategy.
func (s *roundRobinStrategy) Len() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.conns)
}

func (s *roundRobinStrategy) nextIndex() uint64 {
	next := atomic.AddUint64(&s.current, 1)
	return (next - 1) % uint64(len(s.conns))
}
