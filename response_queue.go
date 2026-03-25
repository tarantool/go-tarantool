package tarantool

import (
	"sync"
	"sync/atomic"
)

const CacheLineSize = 64

type resp struct {
	header Header
	buf    smallBuf
}

// responseQueue is a ring-buffer for responses.
type responseQueue struct {
	buf  []resp
	cap  uint64
	mask uint64

	_ [CacheLineSize - 40]byte

	decodeIdx      uint64
	fetchIdxCached uint64

	_ [CacheLineSize - 16]byte

	fetchIdx        uint64
	decodeIdxCached uint64

	_ [CacheLineSize - 16]byte

	mu   sync.Mutex
	cond *sync.Cond

	decodeWaiting int32
	fetchWaiting  int32
}

func newResponseQueue(size uint) *responseQueue {
	cap := uint64(1)
	for cap < uint64(size) {
		cap <<= 1
	}

	q := &responseQueue{
		buf:  make([]resp, cap),
		cap:  cap,
		mask: cap - 1,
	}
	q.cond = sync.NewCond(&q.mu)

	return q
}

func (q *responseQueue) push(r resp) {
	for {
		decodeIdx := atomic.LoadUint64(&q.decodeIdx)
		decodeIdxNext := (decodeIdx + 1) & q.mask

		if decodeIdxNext == q.fetchIdxCached {
			q.fetchIdxCached = atomic.LoadUint64(&q.fetchIdx)
			if decodeIdxNext == q.fetchIdxCached {
				q.waitForSpace()
				continue
			}
		}

		q.buf[decodeIdx] = r

		atomic.StoreUint64(&q.decodeIdx, decodeIdxNext)

		if atomic.LoadInt32(&q.fetchWaiting) > 0 {
			q.mu.Lock()
			q.cond.Signal()
			q.mu.Unlock()
		}

		return
	}
}

func (q *responseQueue) waitForSpace() {
	atomic.AddInt32(&q.decodeWaiting, 1)
	defer atomic.AddInt32(&q.decodeWaiting, -1)

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		decodeIdx := atomic.LoadUint64(&q.decodeIdx)
		fetchIdx := atomic.LoadUint64(&q.fetchIdx)

		decodeIdxNext := (decodeIdx + 1) & q.mask

		if decodeIdxNext != fetchIdx {
			q.fetchIdxCached = fetchIdx
			return
		}

		q.cond.Wait()
	}
}

func (q *responseQueue) pop() resp {
	for {
		fetchIdx := atomic.LoadUint64(&q.fetchIdx)

		if fetchIdx == q.decodeIdxCached {
			q.decodeIdxCached = atomic.LoadUint64(&q.decodeIdx)
			if fetchIdx == q.decodeIdxCached {
				q.waitForData()
				continue
			}
		}

		val := q.buf[fetchIdx]

		fetchIdxNext := (fetchIdx + 1) & q.mask
		atomic.StoreUint64(&q.fetchIdx, fetchIdxNext)

		if atomic.LoadInt32(&q.decodeWaiting) > 0 {
			q.mu.Lock()
			q.cond.Signal()
			q.mu.Unlock()
		}

		return val
	}
}

func (q *responseQueue) waitForData() {
	atomic.AddInt32(&q.fetchWaiting, 1)
	defer atomic.AddInt32(&q.fetchWaiting, -1)

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		fetchIdx := atomic.LoadUint64(&q.fetchIdx)
		decodeIdx := atomic.LoadUint64(&q.decodeIdx)

		if fetchIdx != decodeIdx {
			q.decodeIdxCached = decodeIdx
			return
		}

		q.cond.Wait()
	}
}
