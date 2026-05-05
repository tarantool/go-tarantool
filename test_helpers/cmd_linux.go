//go:build linux

package test_helpers

import (
	"os/exec"
	"syscall"
)

// commandKillOnExit returns an *exec.Cmd that will be terminated by the
// kernel when the parent process dies, see
// https://github.com/golang/go/issues/37206. Without this, a panic in a
// test leaves the spawned tarantool process running and blocking the TCP
// port for subsequent runs (issue #147).
func commandKillOnExit(name string, arg ...string) *exec.Cmd {
	cmd := exec.Command(name, arg...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
	}
	return cmd
}
