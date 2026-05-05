package test_helpers

import (
	"fmt"
	"strings"
	"testing"
)

type fakeT struct {
	failed bool
	logs   []string
	helped int
}

func (f *fakeT) Helper()      { f.helped++ }
func (f *fakeT) Failed() bool { return f.failed }
func (f *fakeT) Logf(format string, args ...any) {
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}

func newInstWithLog(lines ...string) *TarantoolInstance {
	inst := &TarantoolInstance{log: newLogTailBuffer(logTailLines)}
	inst.Opts.Listen = "127.0.0.1:1234"
	for _, l := range lines {
		_, _ = fmt.Fprintln(inst.log, l)
	}
	return inst
}

func TestDumpLogsIfFailed_PassedTest(t *testing.T) {
	ft := &fakeT{failed: false}
	DumpLogsIfFailed(ft, newInstWithLog("oops"))
	if len(ft.logs) != 0 {
		t.Fatalf("expected no logs on passing test, got %v", ft.logs)
	}
}

func TestDumpLogsIfFailed_FailedTest(t *testing.T) {
	ft := &fakeT{failed: true}
	DumpLogsIfFailed(ft, newInstWithLog("oops", "more context"))
	if len(ft.logs) != 1 {
		t.Fatalf("expected one log, got %v", ft.logs)
	}
	if !strings.Contains(ft.logs[0], "oops") || !strings.Contains(ft.logs[0], "more context") {
		t.Fatalf("missing log content: %q", ft.logs[0])
	}
	if !strings.Contains(ft.logs[0], "127.0.0.1:1234") {
		t.Fatalf("missing listen address: %q", ft.logs[0])
	}
}

func TestDumpLogsIfFailed_NilInstance(t *testing.T) {
	ft := &fakeT{failed: true}
	DumpLogsIfFailed(ft, nil)
	if len(ft.logs) != 0 {
		t.Fatalf("expected no logs for nil instance, got %v", ft.logs)
	}
}

func TestDumpLogsIfFailed_EmptyLog(t *testing.T) {
	ft := &fakeT{failed: true}
	DumpLogsIfFailed(ft, newInstWithLog())
	if len(ft.logs) != 0 {
		t.Fatalf("expected no logs for empty buffer, got %v", ft.logs)
	}
}

func TestTarantoolInstance_LogTail_Nil(t *testing.T) {
	var inst *TarantoolInstance
	if got := inst.LogTail(); got != "" {
		t.Fatalf("nil receiver: got %q", got)
	}
	inst2 := &TarantoolInstance{}
	if got := inst2.LogTail(); got != "" {
		t.Fatalf("no buffer: got %q", got)
	}
}
