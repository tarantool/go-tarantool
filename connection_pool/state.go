package connection_pool

import (
	"sync/atomic"
)

// pool state
type state uint32

const (
	unknownState state = iota
	connectedState
	shutdownState
	closedState
)

func (s *state) set(news state) {
	atomic.StoreUint32((*uint32)(s), uint32(news))
}

func (s *state) cas(olds, news state) bool {
	return atomic.CompareAndSwapUint32((*uint32)(s), uint32(olds), uint32(news))
}

func (s *state) get() state {
	return state(atomic.LoadUint32((*uint32)(s)))
}
