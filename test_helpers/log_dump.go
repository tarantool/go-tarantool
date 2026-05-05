package test_helpers

// failedLogger is the subset of *testing.T needed by DumpLogsIfFailed.
// Kept separate from T so adding Failed/Logf does not change the public
// T interface contract.
type failedLogger interface {
	Helper()
	Failed() bool
	Logf(format string, args ...any)
}

// DumpLogsIfFailed prints the tail of the tarantool instance's captured
// stdout/stderr via t.Logf when the test has already failed. Intended
// for use as `defer test_helpers.DumpLogsIfFailed(t, inst)` right after
// a successful StartTarantool, so an assertion failure later in the
// test surfaces the corresponding tarantool log alongside the failure.
//
// No-op when the test passed, the instance is nil, or no log was captured.
func DumpLogsIfFailed(t failedLogger, inst *TarantoolInstance) {
	t.Helper()
	if !t.Failed() || inst == nil {
		return
	}
	tail := inst.LogTail()
	if tail == "" {
		return
	}
	t.Logf("tarantool %q log tail:\n%s", inst.Opts.Listen, tail)
}
