package tarantool

import (
	"errors"
	"math/bits"
	"slices"
	"sync"
)

// PoolAllocator implements the Allocator interface using a set of sync.Pool
// instances for different power-of-two sized byte slices.
//
// The exponents parameter in NewPoolAllocator specifies the powers of two for
// the pool sizes. For example, []int{8, 10, 12} creates pools for 256, 1024,
// and 4096 byte slices.
type PoolAllocator struct {
	pool []*sync.Pool
	size []int
	help []int
}

var _ Allocator = (*PoolAllocator)(nil)

// NewPoolAllocator creates a new PoolAllocator with the given exponents.
// Each exponent represents a power of two pool size. For example, exponent 10
// creates a pool for 1024-byte slices.
//
// Exponents must be sorted in ascending order and each exponent must be
// in range [0, 31].
func NewPoolAllocator(exponents []int) (*PoolAllocator, error) {
	hSize := 32

	for i, s := range exponents {
		if s < 0 || s >= hSize {
			return nil, errors.New("exponent must be in range [0, 31]")
		}

		if i > 0 && exponents[i-1] >= s {
			return nil, errors.New("exponents must be sorted in ascending order")
		}
	}

	var p = PoolAllocator{
		size: make([]int, len(exponents)),
		pool: make([]*sync.Pool, len(exponents)),
		help: slices.Repeat([]int{-1}, hSize),
	}

	for i, s := range exponents {
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

	return &p, nil
}

func (p *PoolAllocator) getInd(size int) int {
	if size <= 0 {
		return -1
	}

	idx := bits.Len(uint(size - 1))
	if idx >= len(p.help) {
		return -1
	}

	return p.help[idx]
}

// Get returns a pointer to a byte slice of at least the given length.
// If the requested size fits within one of the pool sizes, it returns a
// slice from the appropriate pool.
//
// It returns nil if:
//   - length is less than or equal to zero
//   - length exceeds the maximum pool size
func (p *PoolAllocator) Get(length int) *[]byte {
	if length <= 0 {
		return nil
	}

	ind := p.getInd(length)
	if ind == -1 {
		return nil
	}

	bs := p.pool[ind].Get().(*[]byte)
	*bs = (*bs)[:length]
	return bs
}

// Put returns a byte slice to the appropriate pool for reuse.
// The slice is cleared before being returned to the pool.
// If the slice size doesn't match any pool, it is discarded.
// If buf is nil, the method does nothing.
func (p *PoolAllocator) Put(buf *[]byte) {
	if buf == nil {
		return
	}

	if ind := p.getInd(len(*buf)); ind != -1 {
		clear((*buf)[:len(*buf)])
		p.pool[ind].Put(buf)
	}
}
