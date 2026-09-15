package test_helpers

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// newInstWithBrokenWorkDir returns an instance whose options point at a work
// directory path that is already occupied by a regular file. StartTarantool
// then fails while preparing that directory, before it forks a process, so
// the failure needs no Tarantool binary and costs no time.
func newInstWithBrokenWorkDir(t *testing.T) *TarantoolInstance {
	t.Helper()

	workDir := filepath.Join(t.TempDir(), "not_a_dir")
	require.NoError(t, os.WriteFile(workDir, []byte("occupied"), 0o600))

	return &TarantoolInstance{
		Cmd: &exec.Cmd{},
		Opts: StartOpts{
			Listen:  "127.0.0.1:0",
			WorkDir: workDir,
		},
	}
}

func TestRestartTarantool_StartFails(t *testing.T) {
	inst := newInstWithBrokenWorkDir(t)

	var err error
	require.NotPanics(t, func() { err = RestartTarantool(inst) }, "restart must not panic")
	require.Error(t, err, "expected the start error to be returned")
	require.NotNil(t, inst.Cmd, "the instance must be left usable on failure")
	require.Nil(t, inst.Cmd.Process, "no process must be adopted from a failed start")
}

func TestRestartTarantool_NoInstance(t *testing.T) {
	require.NotPanics(t, func() {
		require.Error(t, RestartTarantool(nil), "nil instance")
		require.Error(t, RestartTarantool(&TarantoolInstance{}), "instance without a command")
	})
}
