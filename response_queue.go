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

type Queue struct {
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

	closed int32
}

func NewResponseQueue(size uint) *Queue {
	cap := uint64(1)
	for cap < uint64(size) {
		cap <<= 1
	}

	q := &Queue{
		buf:  make([]resp, cap),
		cap:  cap,
		mask: cap - 1,
	}
	q.cond = sync.NewCond(&q.mu)

	return q
}

func (q *Queue) Push(el resp) {
	if atomic.LoadInt32(&q.closed) == 1 {
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

func (q *Queue) waitForSpace() {
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

		if atomic.LoadInt32(&q.closed) == 1 {
			return
		}

		q.cond.Wait()
	}
}

func (q *Queue) Pop() resp {
	if atomic.LoadInt32(&q.closed) == 1 {
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

func (q *Queue) waitForData() {
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

		if atomic.LoadInt32(&q.closed) == 1 {
			return
		}

		q.cond.Wait()
	}
}

func (q *Queue) Front() (resp, bool) {
	rIdx := atomic.LoadUint64(&q.rIdx)

	if rIdx == q.wIdxCached {
		q.wIdxCached = atomic.LoadUint64(&q.wIdx)
		if rIdx == q.wIdxCached {
			var t resp
			return t, false
		}
	}

	return q.buf[rIdx], true
}

func (q *Queue) Advance() {
	rIdxNext := (q.rIdx + 1) & q.mask
	atomic.StoreUint64(&q.rIdx, rIdxNext)

	if atomic.LoadInt32(&q.producersWaiting) > 0 {
		q.mu.Lock()
		q.cond.Signal()
		q.mu.Unlock()
	}
}

func (q *Queue) Close() {
	atomic.StoreInt32(&q.closed, 1)
	q.mu.Lock()
	q.cond.Broadcast()
	q.mu.Unlock()
}
