package pool

// StrategyBuilder creates strategies for the connection pool.
// It allows users to customize how connections are selected.
type StrategyBuilder interface {
	// Build creates strategies for RW, RO, and ANY modes.
	// ExpectedSize is the expected number of connections for pre-allocation.
	Build(expectedSize int) (rw, ro, any Strategy)
}

// RoundRobinBuilder creates round-robin strategies for all modes.
// This is the default strategy builder.
type RoundRobinBuilder struct{}

// Build creates round-robin strategies.
func (b RoundRobinBuilder) Build(expectedSize int) (rw, ro, any Strategy) {
	return newRoundRobinStrategy(expectedSize),
		newRoundRobinStrategy(expectedSize),
		newRoundRobinStrategy(expectedSize)
}

// ActiveStandbyBuilder creates active/standby strategies for all modes.
//
// ActiveStandby maintains a limited number of "active" connections that
// receive traffic. When an active connection is removed, a standby
// connection is automatically promoted.
type ActiveStandbyBuilder struct {
	// RWPrimaryCount is the maximum number of active connections for RW mode.
	RWPrimaryCount int

	// ROPrimaryCount is the maximum number of active connections for RO mode.
	ROPrimaryCount int
}

// Build creates active/standby strategies for all modes.
// AnyPrimaryCount is calculated as RWPrimaryCount + ROPrimaryCount.
func (b ActiveStandbyBuilder) Build(expectedSize int) (rw, ro, any Strategy) {
	anyPrimaryCount := b.RWPrimaryCount + b.ROPrimaryCount

	return newActiveStandbyStrategy(b.RWPrimaryCount, expectedSize),
		newActiveStandbyStrategy(b.ROPrimaryCount, expectedSize),
		newActiveStandbyStrategy(anyPrimaryCount, expectedSize)
}
