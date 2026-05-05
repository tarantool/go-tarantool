package test_helpers

import (
	"fmt"
	"strings"
	"testing"
)

func TestLogTailBuffer_KeepsLastNLines(t *testing.T) {
	b := newLogTailBuffer(3)
	for i := 1; i <= 5; i++ {
		_, _ = fmt.Fprintf(b, "line %d\n", i)
	}
	got := b.Tail()
	want := "line 3\nline 4\nline 5\n"
	if got != want {
		t.Fatalf("Tail mismatch:\n got: %q\nwant: %q", got, want)
	}
}

func TestLogTailBuffer_PartialLine(t *testing.T) {
	b := newLogTailBuffer(5)
	if _, err := b.Write([]byte("hello ")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := b.Write([]byte("world")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := b.Tail()
	if got != "hello world\n" {
		t.Fatalf("Tail mismatch: got %q", got)
	}
}

func TestLogTailBuffer_SplitWrites(t *testing.T) {
	b := newLogTailBuffer(5)
	_, _ = b.Write([]byte("first\nsecon"))
	_, _ = b.Write([]byte("d\nthird\n"))
	got := b.Tail()
	want := "first\nsecond\nthird\n"
	if got != want {
		t.Fatalf("Tail mismatch:\n got: %q\nwant: %q", got, want)
	}
}

func TestLogTailBuffer_Empty(t *testing.T) {
	b := newLogTailBuffer(5)
	if got := b.Tail(); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
	if got := formatLogTail(b); got != "" {
		t.Fatalf("expected empty formatLogTail, got %q", got)
	}
	if got := formatLogTail(nil); got != "" {
		t.Fatalf("expected empty formatLogTail(nil), got %q", got)
	}
}

func TestFormatLogTail_Wraps(t *testing.T) {
	b := newLogTailBuffer(5)
	_, _ = b.Write([]byte("boom\n"))
	got := formatLogTail(b)
	if !strings.Contains(got, "--- last tarantool log lines ---") {
		t.Fatalf("missing header: %q", got)
	}
	if !strings.Contains(got, "boom") {
		t.Fatalf("missing line: %q", got)
	}
	if !strings.HasSuffix(got, "--- end of tarantool log ---") {
		t.Fatalf("missing footer: %q", got)
	}
}
