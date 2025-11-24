package tarantool

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestPoolerGetInd(t *testing.T) {
	p := newPooler([]int{4, 8, 10, 12})

	for _, tc := range []struct {
		input    int
		expected int
	}{
		{input: -100, expected: -1},
		{input: 0, expected: -1},
		{input: 1, expected: 0},
		{input: 16, expected: 0},
		{input: 17, expected: 1},
		{input: 32, expected: 1},
		{input: 64, expected: 1},
		{input: 65, expected: 1},
		{input: 256, expected: 1},
		{input: 257, expected: 2},
		{input: 1000, expected: 2},
		{input: 1024, expected: 2},
		{input: 1025, expected: 3},
		{input: 3000, expected: 3},
		{input: 3454, expected: 3},
		{input: 4096, expected: 3},
		{input: 4097, expected: -1},
		{input: 8000, expected: -1},
		{input: 16000, expected: -1},
		{input: 32000, expected: -1},
	} {
		assert.Equal(t, tc.expected, p.getInd(tc.input), "input", tc.input)
	}
}

func TestPoolerGetPutSlice(t *testing.T) {
	p := newPooler([]int{4, 8, 10, 12})

	for _, tc := range []struct {
		size  int
		times int
		cap   int
	}{
		{size: 10, times: 5000, cap: p.size[0]},
		{size: 64, times: 5000, cap: p.size[1]},
		{size: 256, times: 5000, cap: p.size[1]},
		{size: 1024, times: 5000, cap: p.size[2]},
		{size: 4096, times: 4000, cap: p.size[3]},
	} {
		buf := p.getSlice(tc.size)
		assert.Len(t, *buf, tc.size)
		assert.Equal(t, cap(*buf), tc.cap)
		ptr := buf

		for range tc.times {
			p.putSlice(buf)

			buf = p.getSlice(tc.size)
			assert.Equal(t, ptr, buf)
		}
	}
}

func TestPoolerSliceClears(t *testing.T) {
	p := newPooler([]int{4, 8, 10, 12})

	data := []byte("eficent work, must be empty after put back")
	size := len(data)

	for {
		buf := p.getSlice(size)

		copy(*buf, data)
		assert.Equal(t, data, *buf)
		p.putSlice(buf)

		another := p.getSlice(size)
		if unsafe.SliceData(*buf) != unsafe.SliceData(*another) {
			p.putSlice(another)
			continue
		}

		if !bytes.Equal(*another, make([]byte, size)) {
			t.Fatalf("slice not cleared")
		}

		break
	}
}
