package tarantool

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPoolAllocator_ValidExponents(t *testing.T) {
	p, err := NewPoolAllocator([]int{4, 8, 12})
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestNewPoolAllocator_EmptyExponents(t *testing.T) {
	p, err := NewPoolAllocator([]int{})
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestNewPoolAllocator_NilExponents(t *testing.T) {
	p, err := NewPoolAllocator(nil)
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestNewPoolAllocator_SingleExponent(t *testing.T) {
	p, err := NewPoolAllocator([]int{10})
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestNewPoolAllocator_BoundaryExponents(t *testing.T) {
	p, err := NewPoolAllocator([]int{0, 31})
	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestNewPoolAllocator_NegativeExponent(t *testing.T) {
	p, err := NewPoolAllocator([]int{4, -1, 8})
	require.Error(t, err)
	assert.Nil(t, p)
	assert.Contains(t, err.Error(), "exponent must be in range [0, 31]")
}

func TestNewPoolAllocator_ExponentTooLarge(t *testing.T) {
	p, err := NewPoolAllocator([]int{4, 32, 8})
	require.Error(t, err)
	assert.Nil(t, p)
	assert.Contains(t, err.Error(), "exponent must be in range [0, 31]")
}

func TestNewPoolAllocator_NotSorted(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 4, 12})
	require.Error(t, err)
	assert.Nil(t, p)
	assert.Contains(t, err.Error(), "exponents must be sorted in ascending order")
}

func TestNewPoolAllocator_DuplicateExponents(t *testing.T) {
	p, err := NewPoolAllocator([]int{4, 4, 8})
	require.Error(t, err)
	assert.Nil(t, p)
	assert.Contains(t, err.Error(), "exponents must be sorted in ascending order")
}

func TestPoolAllocator_GetValid(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	buf := p.Get(100)
	require.NotNil(t, buf)
	assert.GreaterOrEqual(t, cap(*buf), 100)
	assert.Len(t, *buf, 100)
}

func TestPoolAllocator_GetExactSize(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	buf := p.Get(256)
	require.NotNil(t, buf)
	assert.GreaterOrEqual(t, cap(*buf), 256)
	assert.Len(t, *buf, 256)
}

func TestPoolAllocator_GetZero(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	buf := p.Get(0)
	assert.Nil(t, buf)
}

func TestPoolAllocator_GetNegative(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	buf := p.Get(-1)
	assert.Nil(t, buf)
}

func TestPoolAllocator_GetExceedsMax(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	buf := p.Get(10000)
	assert.Nil(t, buf)
}

func TestPoolAllocator_GetEmptyAllocator(t *testing.T) {
	p, err := NewPoolAllocator([]int{})
	require.NoError(t, err)

	buf := p.Get(100)
	assert.Nil(t, buf)
}

func TestPoolAllocator_GetNilAllocator(t *testing.T) {
	p, err := NewPoolAllocator(nil)
	require.NoError(t, err)

	buf := p.Get(100)
	assert.Nil(t, buf)
}

func TestPoolAllocator_GetBufferCapacity(t *testing.T) {
	p, err := NewPoolAllocator([]int{10, 12})
	require.NoError(t, err)

	buf := p.Get(500)
	require.NotNil(t, buf)
	assert.GreaterOrEqual(t, cap(*buf), 500)
	assert.Len(t, *buf, 500)
}

func TestPoolAllocator_PutAndReuse(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	buf1 := p.Get(256)
	require.NotNil(t, buf1)
	ptr := unsafe.SliceData(*buf1)

	p.Put(buf1)

	buf2 := p.Get(256)
	require.NotNil(t, buf2)
	ptr2 := unsafe.SliceData(*buf2)

	assert.Equal(t, ptr, ptr2, "buffer should be reused from pool")
}

func TestPoolAllocator_PutClearsBuffer(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	data := []byte("test data")
	size := len(data)

	buf := p.Get(size)
	require.NotNil(t, buf)
	copy(*buf, data)
	assert.Equal(t, data, *buf)

	p.Put(buf)

	another := p.Get(size)
	require.NotNil(t, another)
	if unsafe.SliceData(*buf) == unsafe.SliceData(*another) {
		for i := range *another {
			assert.Equal(t, byte(0), (*another)[i], "buffer should be cleared after Put")
		}
	}
	p.Put(another)
}

func TestPoolAllocator_PutExceedsMax(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	buf := p.Get(10000)
	assert.Nil(t, buf)

	largeBuf := make([]byte, 10000)
	p.Put(&largeBuf)
}

func TestPoolAllocator_MultipleGetPut(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 12})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		buf := p.Get(100)
		require.NotNil(t, buf)
		assert.GreaterOrEqual(t, cap(*buf), 100)
		p.Put(buf)
	}
}

func TestPoolAllocator_Concurrent(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				buf := p.Get(256)
				if buf != nil {
					p.Put(buf)
				}
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestPoolAllocator_ImplementsAllocator(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	var _ Allocator = p
}

func TestPoolAllocator_DifferentSizes(t *testing.T) {
	p, err := NewPoolAllocator([]int{4, 8, 10, 12})
	require.NoError(t, err)

	sizes := []int{16, 64, 256, 1024, 4096}

	for _, size := range sizes {
		buf := p.Get(size)
		require.NotNil(t, buf, "size %d should return non-nil buffer", size)
		assert.GreaterOrEqual(t, cap(*buf), size)
		assert.Len(t, *buf, size)
		p.Put(buf)
	}
}

func TestPoolAllocator_PutNilBuffer(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	p.Put(nil)
}

func TestPoolAllocator_GetFromSpecificPools(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	buf256 := p.Get(256)
	require.NotNil(t, buf256)
	assert.GreaterOrEqual(t, cap(*buf256), 256)
	assert.LessOrEqual(t, cap(*buf256), 1024)

	buf1024 := p.Get(1024)
	require.NotNil(t, buf1024)
	assert.GreaterOrEqual(t, cap(*buf1024), 1024)
	assert.LessOrEqual(t, cap(*buf1024), 4096)

	p.Put(buf256)
	p.Put(buf1024)
}

func TestPoolAllocator_ConcurrentMultipleSizes(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 10, 12})
	require.NoError(t, err)

	var wg sync.WaitGroup
	sizes := []int{100, 256, 500, 1024, 2000, 4096}

	for _, size := range sizes {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				buf := p.Get(s)
				if buf != nil {
					p.Put(buf)
				}
			}
		}(size)
	}

	wg.Wait()
}

func TestPoolAllocator_SizeBetweenPools(t *testing.T) {
	p, err := NewPoolAllocator([]int{8, 12})
	require.NoError(t, err)

	buf := p.Get(500)
	require.NotNil(t, buf)
	assert.GreaterOrEqual(t, cap(*buf), 500)
	assert.LessOrEqual(t, cap(*buf), 4096)

	p.Put(buf)
}
