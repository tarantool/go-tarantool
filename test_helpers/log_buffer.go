package test_helpers

import (
	"bytes"
	"strings"
	"sync"
)

// logTailLines is the number of trailing log lines kept for a started
// tarantool instance. Shown in the StartTarantool error to help diagnose
// failures from CI logs without re-running the test.
const logTailLines = 50

// logTailBuffer is an io.Writer that keeps the last logTailLines lines
// written to it. Safe for concurrent writes from exec.Cmd's stdout and
// stderr copy goroutines.
type logTailBuffer struct {
	mu      sync.Mutex
	lines   []string
	pending []byte
	max     int
}

func newLogTailBuffer(maxLines int) *logTailBuffer {
	return &logTailBuffer{max: maxLines}
}

func (b *logTailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pending = append(b.pending, p...)
	for {
		i := bytes.IndexByte(b.pending, '\n')
		if i < 0 {
			break
		}
		b.appendLine(string(b.pending[:i]))
		b.pending = b.pending[i+1:]
	}
	return len(p), nil
}

func (b *logTailBuffer) appendLine(line string) {
	if len(b.lines) >= b.max {
		copy(b.lines, b.lines[1:])
		b.lines = b.lines[:len(b.lines)-1]
	}
	b.lines = append(b.lines, line)
}

// formatLogTail returns a "\n--- last N lines of tarantool log ---\n..."
// suffix suitable for appending to a StartTarantool error. Returns an
// empty string when the buffer captured nothing.
func formatLogTail(b *logTailBuffer) string {
	if b == nil {
		return ""
	}
	tail := b.Tail()
	if tail == "" {
		return ""
	}
	return "\n--- last tarantool log lines ---\n" + tail + "--- end of tarantool log ---"
}

// Tail returns the buffered output as a single string with a trailing
// newline if non-empty, including any partial line that was not yet
// terminated by '\n'.
func (b *logTailBuffer) Tail() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.lines) == 0 && len(b.pending) == 0 {
		return ""
	}

	var sb strings.Builder
	for _, l := range b.lines {
		sb.WriteString(l)
		sb.WriteByte('\n')
	}
	if len(b.pending) > 0 {
		sb.Write(b.pending)
		sb.WriteByte('\n')
	}
	return sb.String()
}
