package tarantool

import (
	"sync"
	"sync/atomic"
)

const CacheLineSize = 64

type CacheLinePad [CacheLineSize]byte

type resp struct {
	header Header
	buf    smallBuf
}

type ResponseQueue struct {
	_    CacheLinePad
	buf  []resp
	cap  uint64
	mask uint64

	_    CacheLinePad
	wIdx uint64

	rIdxCached uint64

	_ CacheLinePad

	_    CacheLinePad
	rIdx uint64

	wIdxCached uint64

	_ CacheLinePad

	mu   sync.Mutex
	cond *sync.Cond

	producersWaiting int32
	consumersWaiting int32

	closed atomic.Bool
}

func NewResponseQueue(size uint) *ResponseQueue {
	cap := uint64(1)
	for cap < uint64(size) {
		cap <<= 1
	}

	q := &ResponseQueue{
		buf:  make([]resp, cap),
		cap:  cap,
		mask: cap - 1,
	}
	q.cond = sync.NewCond(&q.mu)

	return q
}

func (q *ResponseQueue) Push(el resp) {
	if q.closed.Load() {
		return
	}

	for {
		wIdx := atomic.LoadUint64(&q.wIdx)
		wIdxNext := (wIdx + 1) & q.mask

		if wIdxNext == q.rIdxCached {
			q.rIdxCached = atomic.LoadUint64(&q.rIdx)
			if wIdxNext == q.rIdxCached {
				q.waitForSpace()
				continue
			}
		}

		q.buf[wIdx] = el

		atomic.StoreUint64(&q.wIdx, wIdxNext)

		if atomic.LoadInt32(&q.consumersWaiting) > 0 {
			q.mu.Lock()
			q.cond.Signal()
			q.mu.Unlock()
		}

		return
	}
}

func (q *ResponseQueue) waitForSpace() {
	atomic.AddInt32(&q.producersWaiting, 1)
	defer atomic.AddInt32(&q.producersWaiting, -1)

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		wIdx := atomic.LoadUint64(&q.wIdx)
		wIdxNext := (wIdx + 1) & q.mask
		rIdx := atomic.LoadUint64(&q.rIdx)

		if wIdxNext != rIdx {
			q.rIdxCached = rIdx
			return
		}

		if q.closed.Load() {
			return
		}

		q.cond.Wait()
	}
}

func (q *ResponseQueue) Pop() resp {
	if q.closed.Load() {
		var t resp
		return t
	}

	for {
		rIdx := atomic.LoadUint64(&q.rIdx)

		if rIdx == q.wIdxCached {
			q.wIdxCached = atomic.LoadUint64(&q.wIdx)
			if rIdx == q.wIdxCached {
				q.waitForData()
				continue
			}
		}

		val := q.buf[rIdx]

		rIdxNext := (rIdx + 1) & q.mask
		atomic.StoreUint64(&q.rIdx, rIdxNext)

		if atomic.LoadInt32(&q.producersWaiting) > 0 {
			q.mu.Lock()
			q.cond.Signal()
			q.mu.Unlock()
		}

		return val
	}
}

func (q *ResponseQueue) waitForData() {
	atomic.AddInt32(&q.consumersWaiting, 1)
	defer atomic.AddInt32(&q.consumersWaiting, -1)

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		rIdx := atomic.LoadUint64(&q.rIdx)
		wIdx := atomic.LoadUint64(&q.wIdx)

		if rIdx != wIdx {
			q.wIdxCached = wIdx
			return
		}

		if q.closed.Load() {
			return
		}

		q.cond.Wait()
	}
}

func (q *ResponseQueue) Close() {
	q.closed.Store(true)
	q.mu.Lock()
	q.cond.Broadcast()
	q.mu.Unlock()
}
