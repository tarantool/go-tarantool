package tarantool

import "sync/atomic"

const cacheLineSize = 64

type resp struct {
	header Header
	buf    smallBuf
}

// ResponseQueue is an SPSC ring buffer.
type ResponseQueue struct {
	buf  []resp
	mask uint64

	_    [cacheLineSize]byte
	wIdx atomic.Uint64
	_    [cacheLineSize]byte
	rIdx atomic.Uint64
	_    [cacheLineSize]byte

	rIdxCached uint64

	reservedIdx uint64
	reserved    bool

	wIdxCached uint64
}

// NewResponseQueue creates a new ResponseQueue with the given capacity (must be a power of two).
func NewResponseQueue(capacity uint64) *ResponseQueue {
	if capacity == 0 || (capacity&(capacity-1)) != 0 {
		panic("capacity must be a power of two")
	}
	return &ResponseQueue{
		buf:  make([]resp, capacity),
		mask: capacity - 1,
	}
}

func (q *ResponseQueue) next(i uint64) uint64 {
	return (i + 1) & q.mask
}

// Push writes an element, spinning until space is available.
func (q *ResponseQueue) Push(el resp) {
	head := q.wIdx.Load()
	next := q.next(head)

	for next == q.rIdxCached {
		q.rIdxCached = q.rIdx.Load()
		if next != q.rIdxCached {
			break
		}
	}

	q.buf[head] = el
	q.reserved = false
	q.wIdx.Store(next)
}

// Offer writes an element without blocking.
func (q *ResponseQueue) Offer(el resp) bool {
	head := q.wIdx.Load()
	next := q.next(head)

	if next == q.rIdxCached {
		q.rIdxCached = q.rIdx.Load()
		if next == q.rIdxCached {
			return false
		}
	}

	q.buf[head] = el
	q.reserved = false
	q.wIdx.Store(next)
	return true
}

// Reserve returns a writable slot without publishing it yet.
func (q *ResponseQueue) Reserve() (*resp, bool) {
	head := q.wIdx.Load()
	next := q.next(head)

	if next == q.rIdxCached {
		q.rIdxCached = q.rIdx.Load()
		if next == q.rIdxCached {
			return nil, false
		}
	}

	q.reservedIdx = next
	q.reserved = true
	return &q.buf[head], true
}

// Commit publishes the slot returned by Reserve.
func (q *ResponseQueue) Commit() {
	if !q.reserved {
		return
	}
	q.reserved = false
	q.wIdx.Store(q.reservedIdx)
}

// Pop reads an element, spinning until one is available.
func (q *ResponseQueue) Pop() resp {
	tail := q.rIdx.Load()

	for tail == q.wIdxCached {
		q.wIdxCached = q.wIdx.Load()
		if tail != q.wIdxCached {
			break
		}
	}

	val := q.buf[tail]
	q.rIdx.Store(q.next(tail))
	return val
}

// Front returns the next element without removing it.
func (q *ResponseQueue) Front() (resp, bool) {
	tail := q.rIdx.Load()

	if tail == q.wIdxCached {
		q.wIdxCached = q.wIdx.Load()
		if tail == q.wIdxCached {
			var zero resp
			return zero, false
		}
	}

	return q.buf[tail], true
}

// Advance consumes the element returned by Front.
func (q *ResponseQueue) Advance() {
	tail := q.rIdx.Load()
	q.rIdx.Store(q.next(tail))
}

// Len returns the number of buffered elements.
func (q *ResponseQueue) Len() uint64 {
	rIdx := q.rIdx.Load()
	wIdx := q.wIdx.Load()

	if wIdx == rIdx {
		return 0
	}
	if wIdx > rIdx {
		return wIdx - rIdx
	}
	return (q.mask + 1) - (rIdx - wIdx)
}
