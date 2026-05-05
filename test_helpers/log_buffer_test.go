package test_helpers

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogTailBuffer_KeepsLastNLines(t *testing.T) {
	b := newLogTailBuffer(3)
	for i := 1; i <= 5; i++ {
		_, _ = fmt.Fprintf(b, "line %d\n", i)
	}
	require.Equal(t, "line 3\nline 4\nline 5\n", b.Tail())
}

func TestLogTailBuffer_PartialLine(t *testing.T) {
	b := newLogTailBuffer(5)
	_, err := b.Write([]byte("hello "))
	require.NoError(t, err)
	_, err = b.Write([]byte("world"))
	require.NoError(t, err)
	require.Equal(t, "hello world\n", b.Tail())
}

func TestLogTailBuffer_SplitWrites(t *testing.T) {
	b := newLogTailBuffer(5)
	_, _ = b.Write([]byte("first\nsecon"))
	_, _ = b.Write([]byte("d\nthird\n"))
	require.Equal(t, "first\nsecond\nthird\n", b.Tail())
}

func TestLogTailBuffer_Empty(t *testing.T) {
	b := newLogTailBuffer(5)
	require.Empty(t, b.Tail())
	require.Empty(t, formatLogTail(b))
	require.Empty(t, formatLogTail(nil))
}

func TestFormatLogTail_Wraps(t *testing.T) {
	b := newLogTailBuffer(5)
	_, _ = b.Write([]byte("boom\n"))
	got := formatLogTail(b)
	require.Contains(t, got, "--- last tarantool log lines ---")
	require.Contains(t, got, "boom")
	require.True(t, strings.HasSuffix(got, "--- end of tarantool log ---"),
		"missing footer: %q", got)
}
