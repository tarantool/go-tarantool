package test_helpers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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
	require.Empty(t, ft.logs, "expected no logs on passing test")
}

func TestDumpLogsIfFailed_FailedTest(t *testing.T) {
	ft := &fakeT{failed: true}
	DumpLogsIfFailed(ft, newInstWithLog("oops", "more context"))
	require.Len(t, ft.logs, 1)
	require.Contains(t, ft.logs[0], "oops")
	require.Contains(t, ft.logs[0], "more context")
	require.Contains(t, ft.logs[0], "127.0.0.1:1234")
}

func TestDumpLogsIfFailed_NilInstance(t *testing.T) {
	ft := &fakeT{failed: true}
	DumpLogsIfFailed(ft, nil)
	require.Empty(t, ft.logs, "expected no logs for nil instance")
}

func TestDumpLogsIfFailed_EmptyLog(t *testing.T) {
	ft := &fakeT{failed: true}
	DumpLogsIfFailed(ft, newInstWithLog())
	require.Empty(t, ft.logs, "expected no logs for empty buffer")
}

func TestTarantoolInstance_LogTail_Nil(t *testing.T) {
	var inst *TarantoolInstance
	require.Empty(t, inst.LogTail(), "nil receiver")
	inst2 := &TarantoolInstance{}
	require.Empty(t, inst2.LogTail(), "no buffer")
}
