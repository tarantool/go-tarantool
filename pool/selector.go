package pool

import (
	"github.com/tarantool/go-tarantool/v3"
)

// selector routes connections to appropriate strategies based on role.
// It observes the store for changes and updates strategies accordingly.
type selector struct {
	rwStrategy  Strategy
	roStrategy  Strategy
	anyStrategy Strategy
}

// newSelector creates a new selector.
func newSelector(
	store *Store,
	rwStrategy Strategy,
	roStrategy Strategy,
	anyStrategy Strategy,
) *selector {
	s := &selector{
		rwStrategy:  rwStrategy,
		roStrategy:  roStrategy,
		anyStrategy: anyStrategy,
	}

	store.AddObserver(s)

	return s
}

// OnUpsert implements StoreObserver.
func (s *selector) OnUpsert(name string, curr Entry, prev Entry, existed bool) {
	applyOne(s.anyStrategy, name, curr, prev, existed, eligibleAny)
	applyOne(s.rwStrategy, name, curr, prev, existed, eligibleRW)
	applyOne(s.roStrategy, name, curr, prev, existed, eligibleRO)
}

// OnRemove implements StoreObserver.
func (s *selector) OnRemove(name string) {
	s.anyStrategy.Remove(name)
	s.rwStrategy.Remove(name)
	s.roStrategy.Remove(name)
}

// Select returns a connection based on the mode.
func (s *selector) Select(mode Mode) (*tarantool.Connection, error) {
	switch mode {
	case RW:
		if conn := s.rwStrategy.Next(); conn != nil {
			return conn, nil
		}
		return nil, ErrNoRwInstance

	case RO:
		if conn := s.roStrategy.Next(); conn != nil {
			return conn, nil
		}
		return nil, ErrNoRoInstance

	case PreferRW:
		if conn := s.rwStrategy.Next(); conn != nil {
			return conn, nil
		}
		if conn := s.roStrategy.Next(); conn != nil {
			return conn, nil
		}
		return nil, ErrNoHealthyInstance

	case PreferRO:
		if conn := s.roStrategy.Next(); conn != nil {
			return conn, nil
		}
		if conn := s.rwStrategy.Next(); conn != nil {
			return conn, nil
		}
		return nil, ErrNoHealthyInstance

	default: // ANY
		if conn := s.anyStrategy.Next(); conn != nil {
			return conn, nil
		}
		return nil, ErrNoHealthyInstance
	}
}

// Get returns a connection by name.
func (s *selector) Get(name string) *tarantool.Connection {
	return s.anyStrategy.Get(name)
}

// Connections returns all eligible connections.
func (s *selector) Connections() map[string]*tarantool.Connection {
	return s.anyStrategy.Connections()
}

// ConnectionsByMode returns connections filtered by mode.
func (s *selector) ConnectionsByMode(mode Mode) map[string]*tarantool.Connection {
	switch mode {
	case RW:
		return s.rwStrategy.Connections()
	case RO:
		return s.roStrategy.Connections()
	default:
		return s.anyStrategy.Connections()
	}
}

// IsEmpty checks if there are no connections for the given mode.
func (s *selector) IsEmpty(mode Mode) bool {
	switch mode {
	case ANY:
		return s.anyStrategy.Len() == 0
	case RW:
		return s.rwStrategy.Len() == 0
	case RO:
		return s.roStrategy.Len() == 0
	case PreferRW, PreferRO:
		return s.rwStrategy.Len() == 0 && s.roStrategy.Len() == 0
	default:
		return true
	}
}

func eligibleAny(e Entry) bool {
	return e.Conn != nil && e.healthy
}

func eligibleRW(e Entry) bool {
	return e.Conn != nil && e.healthy && e.Role == MasterRole
}

func eligibleRO(e Entry) bool {
	return e.Conn != nil && e.healthy && e.Role == ReplicaRole
}

func applyOne(
	strategy Strategy,
	name string,
	curr, prev Entry,
	existed bool,
	eligible func(Entry) bool,
) {
	newOk := eligible(curr)
	oldOk := existed && eligible(prev)

	switch {
	case !oldOk && newOk:
		// became eligible => add
		strategy.Add(name, curr.Conn)

	case oldOk && !newOk:
		// became ineligible => remove
		strategy.Remove(name)

	case oldOk && newOk:
		// still eligible; if conn replaced => update pointer
		if prev.Conn != curr.Conn {
			strategy.Add(name, curr.Conn)
		}

	default:
		// still ineligible => do nothing
	}
}
