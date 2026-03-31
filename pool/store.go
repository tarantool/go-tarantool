package pool

import (
	"sync"

	"github.com/tarantool/go-tarantool/v3"
)

// Entry represents a connection with its metadata.
type Entry struct {
	Conn    *tarantool.Connection
	Role    Role
	healthy bool
}

// StoreObserver is notified when store changes.
type StoreObserver interface {
	// OnUpsert is called when a connection is added or updated.
	// name is the connection name.
	// curr is the current entry, prev is the previous entry (if existed).
	// existed is true if the entry already existed.
	OnUpsert(name string, curr Entry, prev Entry, existed bool)

	// OnRemove is called when a connection is removed.
	// name is the connection name.
	OnRemove(name string)
}

// Store is a thread-safe storage for connection entries.
// It notifies observers about changes via StoreObserver interface.
type Store struct {
	mu        sync.RWMutex
	entries   map[string]Entry
	observers []StoreObserver
}

// NewStore creates a new Store.
func NewStore() *Store {
	return &Store{
		entries: make(map[string]Entry),
	}
}

// AddObserver registers an observer.
// Observers are added only during initialization and never removed.
func (s *Store) AddObserver(observer StoreObserver) {
	s.observers = append(s.observers, observer)
}

// Get returns an entry by name.
func (s *Store) Get(name string) (Entry, bool) {
	s.mu.RLock()
	e, ok := s.entries[name]
	s.mu.RUnlock()
	return e, ok
}

// Upsert adds or updates a connection entry.
// It notifies observers with the current and previous entry.
func (s *Store) Upsert(name string, conn *tarantool.Connection, role Role) (Entry, bool) {
	s.mu.Lock()

	var prev Entry
	var existed bool
	if e, ok := s.entries[name]; ok {
		prev = e
		existed = true
	}

	curr := Entry{
		Conn:    conn,
		Role:    role,
		healthy: true,
	}
	s.entries[name] = curr

	s.mu.Unlock()

	// Notify outside of lock
	for _, o := range s.observers {
		o.OnUpsert(name, curr, prev, existed)
	}

	return prev, existed
}

// UpdateHealth updates the health status of a connection.
func (s *Store) UpdateHealth(name string, healthy bool) bool {
	s.mu.Lock()

	e, ok := s.entries[name]
	if !ok {
		s.mu.Unlock()
		return false
	}

	if e.healthy == healthy {
		s.mu.Unlock()
		return true // No change
	}

	prev := e
	e.healthy = healthy
	s.entries[name] = e
	curr := e

	s.mu.Unlock()

	// Notify outside of lock
	for _, o := range s.observers {
		o.OnUpsert(name, curr, prev, true)
	}

	return true
}

// UpdateRole updates the role of a connection.
func (s *Store) UpdateRole(name string, role Role) bool {
	s.mu.Lock()

	e, ok := s.entries[name]
	if !ok {
		s.mu.Unlock()
		return false
	}

	if e.Role == role {
		s.mu.Unlock()
		return true // No change
	}

	prev := e
	e.Role = role
	s.entries[name] = e
	curr := e

	s.mu.Unlock()

	// Notify outside of lock
	for _, o := range s.observers {
		o.OnUpsert(name, curr, prev, true)
	}

	return true
}

// Remove removes a connection entry.
func (s *Store) Remove(name string) (Entry, bool) {
	s.mu.Lock()

	old, ok := s.entries[name]
	if !ok {
		s.mu.Unlock()
		return Entry{}, false
	}

	delete(s.entries, name)
	s.mu.Unlock()

	// Notify outside of lock
	for _, o := range s.observers {
		o.OnRemove(name)
	}

	return old, true
}

// All returns all entries.
func (s *Store) All() map[string]Entry {
	s.mu.RLock()
	result := make(map[string]Entry, len(s.entries))
	for k, v := range s.entries {
		result[k] = v
	}
	s.mu.RUnlock()
	return result
}

// AllHealthy returns all entries with healthy status.
func (s *Store) AllHealthy() map[string]Entry {
	s.mu.RLock()
	result := make(map[string]Entry)
	for k, v := range s.entries {
		if v.healthy {
			result[k] = v
		}
	}
	s.mu.RUnlock()
	return result
}
