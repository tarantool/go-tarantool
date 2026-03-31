package pool

import (
	"testing"

	"github.com/tarantool/go-tarantool/v3"
)

// roundRobinStrategy Tests

func TestRoundRobinStrategy_AddRemove(t *testing.T) {
	s := newRoundRobinStrategy(10)

	ids := []string{"conn1", "conn2"}
	conns := []*tarantool.Connection{{}, {}}

	for i, id := range ids {
		s.Add(id, conns[i])
	}

	for i, id := range ids {
		removed := s.Remove(id)
		if removed != conns[i] {
			t.Errorf("Remove(%q) = %p, want %p", id, removed, conns[i])
		}
	}

	if len(s.Connections()) != 0 {
		t.Errorf("Connections() should be empty, got %d", len(s.Connections()))
	}
}

func TestRoundRobinStrategy_AddUpsert(t *testing.T) {
	s := newRoundRobinStrategy(10)

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}

	s.Add("conn1", conn1)
	s.Add("conn1", conn2) // Should update (upsert)

	conns := s.Connections()
	if len(conns) != 1 {
		t.Errorf("Connections() len = %d, want 1", len(conns))
	}
	if conns["conn1"] != conn2 {
		t.Errorf("Connections()[conn1] = %p, want %p (updated)", conns["conn1"], conn2)
	}
}

func TestRoundRobinStrategy_RemoveNonExistent(t *testing.T) {
	s := newRoundRobinStrategy(10)

	removed := s.Remove("nonexistent")
	if removed != nil {
		t.Errorf("Remove(nonexistent) = %p, want nil", removed)
	}
}

func TestRoundRobinStrategy_Next(t *testing.T) {
	s := newRoundRobinStrategy(10)

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}

	s.Add("conn1", conn1)
	s.Add("conn2", conn2)

	expected := []*tarantool.Connection{conn1, conn2, conn1, conn2}
	for i, want := range expected {
		got := s.Next()
		if got != want {
			t.Errorf("Next() call %d = %p, want %p", i, got, want)
		}
	}
}

func TestRoundRobinStrategy_NextEmpty(t *testing.T) {
	s := newRoundRobinStrategy(10)

	got := s.Next()
	if got != nil {
		t.Errorf("Next() on empty strategy = %p, want nil", got)
	}
}

func TestRoundRobinStrategy_RemoveIndexUpdate(t *testing.T) {
	s := newRoundRobinStrategy(10)

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}
	conn3 := &tarantool.Connection{}

	s.Add("conn1", conn1)
	s.Add("conn2", conn2)
	s.Add("conn3", conn3)

	s.Remove("conn2")

	expected := []*tarantool.Connection{conn1, conn3, conn1, conn3}
	for i, want := range expected {
		got := s.Next()
		if got != want {
			t.Errorf("Next() after remove, call %d = %p, want %p", i, got, want)
		}
	}
}

// activeStandbyStrategy Tests

func TestActiveStandbyStrategy_ActiveSlots(t *testing.T) {
	s := newActiveStandbyStrategy(2, 10)

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}
	conn3 := &tarantool.Connection{}

	s.Add("conn1", conn1)
	s.Add("conn2", conn2)
	s.Add("conn3", conn3) // Should be standby (only 2 active slots).

	if s.Len() != 2 {
		t.Errorf("ActiveCount() = %d, want 2", s.Len())
	}
	if s.StandbyCount() != 1 {
		t.Errorf("StandbyCount() = %d, want 1", s.StandbyCount())
	}
}

func TestActiveStandbyStrategy_PromotionOnRemove(t *testing.T) {
	s := newActiveStandbyStrategy(2, 10)

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}
	conn3 := &tarantool.Connection{}

	s.Add("conn1", conn1)
	s.Add("conn2", conn2)
	s.Add("conn3", conn3) // Standby.

	// Remove active connection.
	removed := s.Remove("conn1")
	if removed != conn1 {
		t.Errorf("Remove(conn1) = %p, want %p", removed, conn1)
	}

	// Standby should be promoted.
	if s.Len() != 2 {
		t.Errorf("ActiveCount() after remove = %d, want 2", s.Len())
	}
	if !s.IsActive("conn3") {
		t.Errorf("conn3 should be promoted after conn1 removal")
	}
}

func TestActiveStandbyStrategy_RemoveNonExistent(t *testing.T) {
	s := newActiveStandbyStrategy(2, 10)

	removed := s.Remove("nonexistent")
	if removed != nil {
		t.Errorf("Remove(nonexistent) = %p, want nil", removed)
	}
}

func TestActiveStandbyStrategy_Next(t *testing.T) {
	s := newActiveStandbyStrategy(2, 10)

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}

	s.Add("conn1", conn1)
	s.Add("conn2", conn2)

	expected := []*tarantool.Connection{conn1, conn2, conn1, conn2}
	for i, want := range expected {
		got := s.Next()
		if got != want {
			t.Errorf("Next() call %d = %p, want %p", i, got, want)
		}
	}
}

func TestActiveStandbyStrategy_NextOnlyActive(t *testing.T) {
	s := newActiveStandbyStrategy(1, 10)

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}

	s.Add("conn1", conn1)
	s.Add("conn2", conn2) // Standby.

	// Next should only return conn1 (active).
	for i := 0; i < 4; i++ {
		got := s.Next()
		if got != conn1 {
			t.Errorf("Next() call %d = %p, want %p (conn1)", i, got, conn1)
		}
	}
}

// selector Tests

type selectorTest struct {
	store       *Store
	rwStrategy  *roundRobinStrategy
	roStrategy  *roundRobinStrategy
	anyStrategy *roundRobinStrategy
	sel         *selector
}

func setupSelectorTest() selectorTest {
	store := NewStore()
	rwStrategy := newRoundRobinStrategy(10)
	roStrategy := newRoundRobinStrategy(10)
	anyStrategy := newRoundRobinStrategy(10)
	sel := newSelector(store, rwStrategy, roStrategy, anyStrategy)
	return selectorTest{
		store:       store,
		rwStrategy:  rwStrategy,
		roStrategy:  roStrategy,
		anyStrategy: anyStrategy,
		sel:         sel,
	}
}

func TestSelector_Select_RW(t *testing.T) {
	test := setupSelectorTest()

	masterConn := &tarantool.Connection{}
	test.store.Upsert("master", masterConn, MasterRole)

	// RW mode should return master.
	conn, err := test.rwStrategy.Next(), error(nil)
	if err != nil {
		t.Errorf("Next() error = %v", err)
	}
	if conn != masterConn {
		t.Errorf("Next() = %p, want %p", conn, masterConn)
	}
}

func TestSelector_Select_RO(t *testing.T) {
	test := setupSelectorTest()

	masterConn := &tarantool.Connection{}
	replicaConn := &tarantool.Connection{}

	test.store.Upsert("master", masterConn, MasterRole)
	test.store.Upsert("replica", replicaConn, ReplicaRole)

	// RO mode should return replica.
	conn := test.roStrategy.Next()
	if conn != replicaConn {
		t.Errorf("Next() = %p, want %p", conn, replicaConn)
	}
}

func TestSelector_Select_ANY(t *testing.T) {
	test := setupSelectorTest()

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}

	test.store.Upsert("conn1", conn1, MasterRole)
	test.store.Upsert("conn2", conn2, ReplicaRole)

	// ANY mode should return any connection.
	seen := make(map[*tarantool.Connection]bool)
	for i := 0; i < 10; i++ {
		conn := test.anyStrategy.Next()
		seen[conn] = true
	}
	// Should have seen both connections.
	if len(seen) != 2 {
		t.Errorf("Next() should rotate between all connections, saw %d unique", len(seen))
	}
}

func TestSelector_RoleChange(t *testing.T) {
	test := setupSelectorTest()

	conn := &tarantool.Connection{}
	test.store.Upsert("conn", conn, MasterRole)

	// Initially master.
	if test.rwStrategy.Next() == nil {
		t.Errorf("rwStrategy.Next() should return connection")
	}

	// Change role to replica.
	test.store.UpdateRole("conn", ReplicaRole)

	// Now should be in RO pool.
	if test.rwStrategy.Next() != nil {
		t.Errorf("rwStrategy.Next() should return nil after role change")
	}
	if test.roStrategy.Next() == nil {
		t.Errorf("roStrategy.Next() should return connection after role change")
	}
}

func TestSelector_Remove(t *testing.T) {
	test := setupSelectorTest()

	conn := &tarantool.Connection{}
	test.store.Upsert("conn", conn, MasterRole)

	test.store.Remove("conn")

	// Should be removed from all strategies.
	if !test.sel.IsEmpty(RW) {
		t.Errorf("IsEmpty(RW) = false, want true")
	}
	if !test.sel.IsEmpty(ANY) {
		t.Errorf("IsEmpty(ANY) = false, want true")
	}
}

func TestSelector_Get(t *testing.T) {
	test := setupSelectorTest()

	conn := &tarantool.Connection{}
	test.store.Upsert("conn", conn, MasterRole)

	got := test.sel.Get("conn")
	if got != conn {
		t.Errorf("Get(conn) = %p, want %p", got, conn)
	}

	got = test.sel.Get("nonexistent")
	if got != nil {
		t.Errorf("Get(nonexistent) = %p, want nil", got)
	}
}

func TestSelector_HealthChange(t *testing.T) {
	test := setupSelectorTest()

	conn := &tarantool.Connection{}
	test.store.Upsert("conn", conn, MasterRole)

	// Initially healthy.
	if test.rwStrategy.Next() == nil {
		t.Errorf("rwStrategy.Next() should return connection")
	}

	// Become unhealthy.
	test.store.UpdateHealth("conn", false)

	// Should not be available.
	if test.rwStrategy.Next() != nil {
		t.Errorf("rwStrategy.Next() should return nil when unhealthy")
	}
	if !test.sel.IsEmpty(RW) {
		t.Errorf("IsEmpty(RW) = false, want true when unhealthy")
	}

	// Become healthy again.
	test.store.UpdateHealth("conn", true)

	// Should be available again.
	if test.rwStrategy.Next() == nil {
		t.Errorf("rwStrategy.Next() should return connection after recovery")
	}
}

func TestSelector_ConnectionUpdate(t *testing.T) {
	test := setupSelectorTest()

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}

	test.store.Upsert("conn", conn1, MasterRole)
	if test.sel.Get("conn") != conn1 {
		t.Errorf("Get(conn) = %p, want %p", test.sel.Get("conn"), conn1)
	}

	// Update connection pointer.
	test.store.Upsert("conn", conn2, MasterRole)
	if test.sel.Get("conn") != conn2 {
		t.Errorf("Get(conn) = %p, want %p", test.sel.Get("conn"), conn2)
	}
}
