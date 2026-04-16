package pool

import (
	"sync"
	"sync/atomic"

	"github.com/tarantool/go-tarantool/v3"
)

// activeStandbyStrategy implements a strategy with active/standby separation.
// Only a subset of connections (primary) are active and receive traffic.
// Standby connections are promoted when active ones are removed.
type activeStandbyStrategy struct {
	primaryCount int

	// All connections
	conns     []*tarantool.Connection
	indexById map[string]uint

	// Active connections for round-robin
	activeConns []*tarantool.Connection
	activeIndex map[string]uint

	mutex   sync.RWMutex
	current uint64
}

// newActiveStandbyStrategy creates a new active/standby strategy.
// primaryCount is the maximum number of active connections.
// expectedSize is used for pre-allocation.
func newActiveStandbyStrategy(primaryCount int, expectedSize int) *activeStandbyStrategy {
	return &activeStandbyStrategy{
		primaryCount: primaryCount,
		conns:        make([]*tarantool.Connection, 0, expectedSize),
		indexById:    make(map[string]uint, expectedSize),
		activeConns:  make([]*tarantool.Connection, 0, primaryCount),
		activeIndex:  make(map[string]uint, primaryCount),
	}
}

// Add adds or updates a connection with the given ID (upsert).
func (s *activeStandbyStrategy) Add(id string, conn *tarantool.Connection) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if idx, exists := s.indexById[id]; exists {
		s.conns[idx] = conn
		if activeIdx, active := s.activeIndex[id]; active {
			s.activeConns[activeIdx] = conn
		}
		return
	}

	s.indexById[id] = uint(len(s.conns))
	s.conns = append(s.conns, conn)

	if len(s.activeConns) < s.primaryCount {
		s.addToActiveLocked(id)
	}
}

// Get returns a connection by ID.
func (s *activeStandbyStrategy) Get(id string) *tarantool.Connection {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	index, exists := s.indexById[id]
	if !exists {
		return nil
	}

	return s.conns[index]
}

// Remove removes a connection by ID.
func (s *activeStandbyStrategy) Remove(id string) *tarantool.Connection {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	index, exists := s.indexById[id]
	if !exists {
		return nil
	}

	conn := s.conns[index]

	if _, active := s.activeIndex[id]; active {
		s.removeFromActiveLocked(id)
	}

	delete(s.indexById, id)
	s.conns = append(s.conns[:index], s.conns[index+1:]...)

	for id, idx := range s.indexById {
		if idx > index {
			s.indexById[id] = idx - 1
		}
	}

	s.promoteStandbyLocked()

	return conn
}

// Next returns the next active connection in round-robin order.
func (s *activeStandbyStrategy) Next() *tarantool.Connection {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if len(s.activeConns) == 0 {
		return nil
	}

	return s.activeConns[s.nextIndex()]
}

func (s *activeStandbyStrategy) nextIndex() uint64 {
	next := atomic.AddUint64(&s.current, 1)
	return (next - 1) % uint64(len(s.activeConns))
}

// Connections returns eligible connections (active only).
func (s *activeStandbyStrategy) Connections() map[string]*tarantool.Connection {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	result := make(map[string]*tarantool.Connection, len(s.activeIndex))
	for id, idx := range s.activeIndex {
		result[id] = s.activeConns[idx]
	}
	return result
}

func (s *activeStandbyStrategy) addToActiveLocked(id string) {
	if _, active := s.activeIndex[id]; active {
		return
	}

	index, exists := s.indexById[id]
	if !exists {
		return
	}

	s.activeIndex[id] = uint(len(s.activeConns))
	s.activeConns = append(s.activeConns, s.conns[index])
}

func (s *activeStandbyStrategy) removeFromActiveLocked(id string) {
	index, active := s.activeIndex[id]
	if !active {
		return
	}

	delete(s.activeIndex, id)
	s.activeConns = append(s.activeConns[:index], s.activeConns[index+1:]...)

	for id, idx := range s.activeIndex {
		if idx > index {
			s.activeIndex[id] = idx - 1
		}
	}
}

func (s *activeStandbyStrategy) promoteStandbyLocked() {
	for len(s.activeConns) < s.primaryCount {
		promoted := false
		for id := range s.indexById {
			if _, active := s.activeIndex[id]; active {
				continue
			}

			s.addToActiveLocked(id)
			promoted = true
			break
		}

		if !promoted {
			break
		}
	}
}

// StandbyCount returns the number of standby connections.
func (s *activeStandbyStrategy) StandbyCount() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.conns) - len(s.activeConns)
}

// IsActive checks if a connection is active.
func (s *activeStandbyStrategy) IsActive(id string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	_, active := s.activeIndex[id]
	return active
}

// Len returns the number of active connections managed by this strategy.
func (s *activeStandbyStrategy) Len() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.activeConns)
}
