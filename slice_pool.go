package tarantool

import (
	"math/bits"
	"slices"
	"sync"
)

var (
	slicePool = newPooler([]int{8, 12, 16})
)

// pooler contains multiple pool's,
// which allocates power-of-two size byte slices.
type pooler struct {
	pool []*sync.Pool
	size []int
	help []int
}

// newPooler creates pooler using
// power-of-two exponentes slice.
func newPooler(size []int) pooler {
	slices.Sort(size)
	size = slices.Compact(size)
	hSize := 32

	var p = pooler{
		size: make([]int, len(size)),
		pool: make([]*sync.Pool, len(size)),
		help: slices.Repeat([]int{-1}, hSize),
	}

	for i, s := range size {
		p.size[i] = 1 << s
		p.help[s] = i
		p.pool[i] = &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, p.size[i])
				return &buf
			},
		}
	}

	for i := hSize - 2; i >= 0; i-- {
		if p.help[i] != -1 {
			continue
		}
		p.help[i] = p.help[i+1]
	}

	return p
}

// getInd returns least index of size slice, which is greater or equal given size.
// In case of non-positive size or size greater than greatest capacity, returns -1.
func (p *pooler) getInd(size int) int {
	if size <= 0 {
		return -1
	}
	return p.help[bits.Len(uint(size-1))]
}

// getSlice returns pointer of byte slice of given size,
// also clear returning slice.
func (p *pooler) getSlice(length int) *[]byte {
	if ind := p.getInd(length); ind != -1 {
		bs := p.pool[ind].Get().(*[]byte)

		clear((*bs)[:cap(*bs)])
		*bs = (*bs)[:length]
		return bs
	}

	b := make([]byte, length)
	return &b
}

// putSlice returning given slice to correct one pool.
func (p *pooler) putSlice(buf *[]byte) {
	if ind := p.getInd(len(*buf)); ind != -1 {
		p.pool[ind].Put(buf)
	}
}
