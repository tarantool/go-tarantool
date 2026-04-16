package tarantool

import "sync/atomic"

// atomicConnectionState provides atomic access to connection state.
// It is not exported because it's an internal implementation detail.
type atomicConnectionState struct {
	state uint32
}

// Load returns the current state.
func (a *atomicConnectionState) Load() ConnectionState {
	return ConnectionState(atomic.LoadUint32(&a.state))
}

// Store sets the state.
func (a *atomicConnectionState) Store(s ConnectionState) {
	atomic.StoreUint32(&a.state, uint32(s))
}

// CompareAndSwap performs atomic compare-and-swap.
func (a *atomicConnectionState) CompareAndSwap(old, new ConnectionState) bool {
	return atomic.CompareAndSwapUint32(&a.state, uint32(old), uint32(new))
}

// Is reports whether the current state equals the given state.
func (a *atomicConnectionState) Is(s ConnectionState) bool {
	return a.Load() == s
}
