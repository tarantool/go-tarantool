//go:build !linux

package test_helpers

import (
	"os/exec"
)

// commandKillOnExit is a fallback for platforms without a Pdeathsig
// equivalent. It just delegates to exec.Command, so a parent panic may
// leave the tarantool child alive (issue #147).
func commandKillOnExit(name string, arg ...string) *exec.Cmd {
	return exec.Command(name, arg...)
}
