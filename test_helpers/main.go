package test_helpers

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"time"

	"github.com/tarantool/go-tarantool"
)

type StartOpts struct {
	// InitScript is a Lua script for tarantool to run on start.
	InitScript string

	// Listen is box.cfg listen parameter for tarantool.
	// Use this address to connect to tarantool after configuration.
	// https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-basic-listen
	Listen string

	// WorkDir is box.cfg work_dir parameter for tarantool.
	// Specify folder to store tarantool data files.
	// Folder must be unique for each tarantool process used simultaneously.
	// https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-work_dir
	WorkDir string

	// User is a username used to connect to tarantool.
	// All required grants must be given in InitScript.
	User string

	// Pass is a password for specified User.
	Pass string

	// WaitStart is a time to wait before starting to ping tarantool.
	WaitStart time.Duration

	// ConnectRetry is a count of attempts to ping tarantool.
	ConnectRetry uint

	// RetryTimeout is a time between tarantool ping retries.
	RetryTimeout time.Duration
}

// TarantoolInstance is a data for instance graceful shutdown and cleanup.
type TarantoolInstance struct {
	// Cmd is a Tarantool command. Used to kill Tarantool process.
	Cmd *exec.Cmd

	// WorkDir is a directory with tarantool data. Cleaned up after run.
	WorkDir string
}

func isReady(server string, opts *tarantool.Opts) error {
	var err error
	var conn *tarantool.Connection
	var resp *tarantool.Response

	conn, err = tarantool.Connect(server, *opts)
	if err != nil {
		return err
	}
	if conn == nil {
		return errors.New("Conn is nil after connect")
	}
	defer conn.Close()

	resp, err = conn.Ping()
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("Response is nil after ping")
	}

	return nil
}

var (
	// Used to extract Tarantool version (major.minor.patch).
	tarantoolVersionRegexp *regexp.Regexp
)

func init() {
	tarantoolVersionRegexp = regexp.MustCompile(`Tarantool (?:Enterprise )?(\d+)\.(\d+)\.(\d+).*`)
}

// atoiUint64 parses string to uint64.
func atoiUint64(str string) (uint64, error) {
	res, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cast to number error (%s)", err)
	}
	return res, nil
}

// IsTarantoolVersionLess checks if tarantool version is less
// than passed <major.minor.patch>. Returns error if failed
// to extract version.
func IsTarantoolVersionLess(majorMin uint64, minorMin uint64, patchMin uint64) (bool, error) {
	var major, minor, patch uint64

	out, err := exec.Command("tarantool", "--version").Output()

	if err != nil {
		return true, err
	}

	parsed := tarantoolVersionRegexp.FindStringSubmatch(string(out))

	if parsed == nil {
		return true, errors.New("regexp parse failed")
	}

	if major, err = atoiUint64(parsed[1]); err != nil {
		return true, fmt.Errorf("failed to parse major: %s", err)
	}

	if minor, err = atoiUint64(parsed[2]); err != nil {
		return true, fmt.Errorf("failed to parse minor: %s", err)
	}

	if patch, err = atoiUint64(parsed[3]); err != nil {
		return true, fmt.Errorf("failed to parse patch: %s", err)
	}

	if major != majorMin {
		return major < majorMin, nil
	} else if minor != minorMin {
		return minor < minorMin, nil
	} else {
		return patch < patchMin, nil
	}

	return false, nil
}

// StartTarantool starts a tarantool instance for tests
// with specifies parameters (refer to StartOpts).
// Process must be stopped with StopTarantool.
func StartTarantool(startOpts StartOpts) (TarantoolInstance, error) {
	// Prepare tarantool command.
	var inst TarantoolInstance
	inst.Cmd = exec.Command("tarantool", startOpts.InitScript)

	inst.Cmd.Env = append(
		os.Environ(),
		fmt.Sprintf("TEST_TNT_WORK_DIR=%s", startOpts.WorkDir),
		fmt.Sprintf("TEST_TNT_LISTEN=%s", startOpts.Listen),
	)

	// Clean up existing work_dir.
	err := os.RemoveAll(startOpts.WorkDir)
	if err != nil {
		return inst, err
	}

	// Create work_dir.
	err = os.Mkdir(startOpts.WorkDir, 0755)
	if err != nil {
		return inst, err
	}

	inst.WorkDir = startOpts.WorkDir

	// Start tarantool.
	err = inst.Cmd.Start()
	if err != nil {
		return inst, err
	}

	// Try to connect and ping tarantool.
	// Using reconnect opts do not help on Connect,
	// see https://github.com/tarantool/go-tarantool/issues/136
	time.Sleep(startOpts.WaitStart)

	opts := tarantool.Opts{
		Timeout:    500 * time.Millisecond,
		User:       startOpts.User,
		Pass:       startOpts.Pass,
		SkipSchema: true,
	}

	var i uint
	for i = 0; i <= startOpts.ConnectRetry; i++ {
		err = isReady(startOpts.Listen, &opts)

		// Both connect and ping is ok.
		if err == nil {
			break
		}

		if i != startOpts.ConnectRetry {
			time.Sleep(startOpts.RetryTimeout)
		}
	}

	return inst, err
}

// StopTarantool stops a tarantool instance started
// with StartTarantool. Waits until any resources
// associated with the process is released. If something went wrong, fails.
func StopTarantool(inst TarantoolInstance) {
	if inst.Cmd != nil && inst.Cmd.Process != nil {
		if err := inst.Cmd.Process.Kill(); err != nil {
			log.Fatalf("Failed to kill tarantool (pid %d), got %s", inst.Cmd.Process.Pid, err)
		}

		// Wait releases any resources associated with the Process.
		if _, err := inst.Cmd.Process.Wait(); err != nil {
			log.Fatalf("Failed to wait for Tarantool process to exit, got %s", err)
		}

		inst.Cmd = nil
	}
}

// StopTarantoolWithCleanup stops a tarantool instance started
// with StartTarantool. Waits until any resources
// associated with the process is released.
// Cleans work directory after stop. If something went wrong, fails.
func StopTarantoolWithCleanup(inst TarantoolInstance) {
	StopTarantool(inst)

	if inst.WorkDir != "" {
		if err := os.RemoveAll(inst.WorkDir); err != nil {
			log.Fatalf("Failed to clean work directory, got %s", err)
		}
	}
}
