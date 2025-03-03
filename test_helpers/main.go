// Helpers for managing Tarantool process for testing purposes.
//
// Package introduces go helpers for starting a tarantool process and
// validating Tarantool version. Helpers are based on os/exec calls.
// Retries to connect test tarantool instance handled explicitly,
// see tarantool/go-tarantool/#136.
//
// Tarantool's instance Lua scripts use environment variables to configure
// box.cfg. Listen port is set in the end of script so it is possible to
// connect only if every other thing was set up already.
package test_helpers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tarantool/go-tarantool/v2"
)

type StartOpts struct {
	// Auth is an authentication method for a Tarantool instance.
	Auth tarantool.Auth

	// InitScript is a Lua script for tarantool to run on start.
	InitScript string

	// ConfigFile is a path to a configuration file for a Tarantool instance.
	// Required in pair with InstanceName.
	ConfigFile string

	// InstanceName is a name of an instance to run.
	// Required in pair with ConfigFile.
	InstanceName string

	// Listen is box.cfg listen parameter for tarantool.
	// Use this address to connect to tarantool after configuration.
	// https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-basic-listen
	Listen string

	// WorkDir is box.cfg work_dir parameter for a Tarantool instance:
	// a folder to store data files. If not specified, helpers create a
	// new temporary directory.
	// Folder must be unique for each Tarantool process used simultaneously.
	// https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-work_dir
	WorkDir string

	// SslCertsDir is a path to a directory with SSL certificates. It will be
	// copied to the working directory.
	SslCertsDir string

	// WaitStart is a time to wait before starting to ping tarantool.
	WaitStart time.Duration

	// ConnectRetry is a count of retry attempts to ping tarantool. If the
	// value < 0 then there will be no ping tarantool at all.
	ConnectRetry int

	// RetryTimeout is a time between tarantool ping retries.
	RetryTimeout time.Duration

	// MemtxUseMvccEngine is flag to enable transactional
	// manager if set to true.
	MemtxUseMvccEngine bool

	// Dialer to check that connection established.
	Dialer tarantool.Dialer
}

// errorWrap should be used with a pointer - as `optional` type.
type errorWrap struct {
	err error
}

type statusInstance struct {
	// status if nil, then process is not done yet.
	status *errorWrap
	// isStopping is a flag that terminate was internally initiated.
	isStopping atomic.Bool
	// waitMutex is used to prevent several invokes of the "Wait"
	// for the same process.
	// https://github.com/golang/go/issues/28461
	waitMutex sync.RWMutex
}

// statusInstanceWrap used to avoid capturing content in defer function.
type statusInstanceWrap struct {
	impl *statusInstance
}

// TarantoolInstance is a data for instance graceful shutdown and cleanup.
type TarantoolInstance struct {
	// Cmd is a Tarantool command. Used to kill Tarantool process.
	Cmd *exec.Cmd

	// Options for restarting a tarantool instance.
	Opts StartOpts

	// Dialer to check that connection established.
	Dialer tarantool.Dialer

	st *statusInstanceWrap
}

func newTarantoolInstance() TarantoolInstance {
	return TarantoolInstance{st: &statusInstanceWrap{&statusInstance{}}}
}

func (t *TarantoolInstance) IsExit() bool {
	succeeded := t.st.impl.waitMutex.TryRLock()
	if !succeeded {
		// Due to mutex locked by goroutine, it means that process running.
		return false
	}
	defer t.st.impl.waitMutex.RUnlock()
	return t.st.impl.status != nil
}

func (t *TarantoolInstance) result() error {
	succeeded := t.st.impl.waitMutex.TryRLock()
	if !succeeded {
		return nil
	}
	defer t.st.impl.waitMutex.RUnlock()
	return t.st.impl.status.err
}

func (t *TarantoolInstance) checkDone() {
	if t.st == nil || t.st.impl == nil {
		panic("TarantoolInstance is not initialized properly.")
	}

	go func() {
		t.st.impl.waitMutex.Lock()
		defer t.st.impl.waitMutex.Unlock()
		t.st.impl.status = &errorWrap{t.Cmd.Wait()}
		if !t.st.impl.isStopping.Load() {
			log.Printf("Tarantool %q was unexpected terminated: %v", t.Opts.Listen, t.result())
		}
	}()
}

func (t *TarantoolInstance) Wait() error {
	if t.st == nil {
		panic("TarantoolInstance is not initialized")
	}
	t.st.impl.waitMutex.RLock()
	defer t.st.impl.waitMutex.RUnlock()
	// Note: don't call `result()` here to avoid double locking.
	return t.st.impl.status.err
}

func (t *TarantoolInstance) Stop() error {
	if t == nil {
		log.Print("ASSERT: no Tarantool instance")
		return nil
	}
	log.Printf("Stopping Tarantool instance %q", t.Opts.Listen)
	t.st.impl.isStopping.Store(true)
	if t.IsExit() {
		log.Printf("Already stopped instance %q with result: %v", t.Opts.Listen, t.result())
		return nil
	}
	if t.Cmd != nil && t.Cmd.Process != nil {
		log.Printf("Killing Tarantool %q (pid %d)", t.Opts.Listen, t.Cmd.Process.Pid)
		if err := t.Cmd.Process.Kill(); err != nil && !t.IsExit() {
			return fmt.Errorf("failed to kill tarantool %q (pid %d), got %s",
				t.Opts.Listen, t.Cmd.Process.Pid, err)
		}
		t.Wait()
		t.Cmd.Process = nil
	}
	return nil
}

func isReady(dialer tarantool.Dialer, opts *tarantool.Opts) error {
	var err error
	var conn *tarantool.Connection

	ctx, cancel := GetConnectContext()
	defer cancel()
	conn, err = tarantool.Connect(ctx, dialer, *opts)
	if err != nil {
		return err
	}
	if conn == nil {
		return errors.New("connection is nil after connect")
	}
	defer conn.Close()

	_, err = conn.Do(tarantool.NewPingRequest()).Get()
	if err != nil {
		return err
	}

	return nil
}

var (
	// Used to extract Tarantool version (major.minor.patch).
	tarantoolVersionRegexp *regexp.Regexp
)

func init() {
	tarantoolVersionRegexp = regexp.MustCompile(`Tarantool (Enterprise )?(\d+)\.(\d+)\.(\d+).*`)
}

// atoiUint64 parses string to uint64.
func atoiUint64(str string) (uint64, error) {
	res, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cast to number error (%s)", err)
	}
	return res, nil
}

func getTarantoolExec() string {
	if tar_bin := os.Getenv("TARANTOOL_BIN"); tar_bin != "" {
		return tar_bin
	}
	return "tarantool"
}

// IsTarantoolVersionLess checks if tarantool version is less
// than passed <major.minor.patch>. Returns error if failed
// to extract version.
func IsTarantoolVersionLess(majorMin uint64, minorMin uint64, patchMin uint64) (bool, error) {
	var major, minor, patch uint64

	out, err := exec.Command(getTarantoolExec(), "--version").Output()

	if err != nil {
		return true, err
	}

	parsed := tarantoolVersionRegexp.FindStringSubmatch(string(out))

	if parsed == nil {
		return true, fmt.Errorf("failed to parse output %q", out)
	}

	if major, err = atoiUint64(parsed[2]); err != nil {
		return true, fmt.Errorf("failed to parse major from output %q: %w", out, err)
	}

	if minor, err = atoiUint64(parsed[3]); err != nil {
		return true, fmt.Errorf("failed to parse minor from output %q: %w", out, err)
	}

	if patch, err = atoiUint64(parsed[4]); err != nil {
		return true, fmt.Errorf("failed to parse patch from output %q: %w", out, err)
	}

	if major != majorMin {
		return major < majorMin, nil
	} else if minor != minorMin {
		return minor < minorMin, nil
	} else {
		return patch < patchMin, nil
	}
}

// IsTarantoolEE checks if Tarantool is Enterprise edition.
func IsTarantoolEE() (bool, error) {
	out, err := exec.Command(getTarantoolExec(), "--version").Output()
	if err != nil {
		return true, err
	}

	parsed := tarantoolVersionRegexp.FindStringSubmatch(string(out))
	if parsed == nil {
		return true, fmt.Errorf("failed to parse output %q", out)
	}

	return parsed[1] != "", nil
}

// RestartTarantool restarts a tarantool instance for tests
// with specifies parameters (refer to StartOpts)
// which were specified in inst parameter.
// inst is a tarantool instance that was started by
// StartTarantool. Rewrites inst.Cmd.Process to stop
// instance with StopTarantool.
// Process must be stopped with StopTarantool.
func RestartTarantool(inst *TarantoolInstance) error {
	log.Printf("Restarting Tarantool instance %q", inst.Opts.Listen)
	startedInst, err := StartTarantool(inst.Opts)
	inst.Cmd.Process = startedInst.Cmd.Process
	// We can't change pointer to status instance, cause it could captured by `defer`.
	inst.st.impl = startedInst.st.impl
	return err
}

func removeByMask(dir string, masks ...string) error {
	for _, mask := range masks {
		files, err := filepath.Glob(filepath.Join(dir, mask))
		if err != nil {
			return err
		}
		for _, f := range files {
			if err = os.Remove(f); err != nil {
				return err
			}
		}
	}
	return nil
}

func prepareDir(workDir string) (string, error) {
	if workDir == "" {
		dir, err := os.MkdirTemp("", "work_dir")
		if err != nil {
			return "", err
		}
		return dir, nil
	}
	// Create work_dir.
	err := os.MkdirAll(workDir, 0755)
	if err != nil {
		return "", err
	}

	// Clean up existing work_dir.
	// TODO: Ensure that nested files will be removed.
	err = removeByMask(workDir, "*.snap", "*.xlog")
	if err != nil {
		return "", err
	}
	return workDir, nil
}

// StartTarantool starts a tarantool instance for tests
// with specifies parameters (refer to StartOpts).
// Process must be stopped with StopTarantool.
func StartTarantool(startOpts StartOpts) (TarantoolInstance, error) {
	// Prepare tarantool command.
	inst := newTarantoolInstance()
	var err error

	inst.Dialer = startOpts.Dialer
	startOpts.WorkDir, err = prepareDir(startOpts.WorkDir)
	if err != nil {
		return inst, fmt.Errorf("failed prepare working dir %q: %w", startOpts.WorkDir, err)
	}

	args := []string{}
	if startOpts.InitScript != "" {
		if !filepath.IsAbs(startOpts.InitScript) {
			cwd, err := os.Getwd()
			if err != nil {
				return inst, fmt.Errorf("failed to get current working directory: %w", err)
			}
			startOpts.InitScript = filepath.Join(cwd, startOpts.InitScript)
		}
		args = append(args, startOpts.InitScript)
	}
	if startOpts.ConfigFile != "" && startOpts.InstanceName != "" {
		args = append(args, "--config", startOpts.ConfigFile)
		args = append(args, "--name", startOpts.InstanceName)
	}
	inst.Cmd = exec.Command(getTarantoolExec(), args...)
	inst.Cmd.Dir = startOpts.WorkDir
	inst.Cmd.Stdout = os.Stderr //! DEBUG: remove
	inst.Cmd.Stderr = os.Stderr //! DEBUG: remove

	inst.Cmd.Env = append(
		os.Environ(),
		fmt.Sprintf("TEST_TNT_WORK_DIR=%s", startOpts.WorkDir),
		fmt.Sprintf("TEST_TNT_LISTEN=%s", startOpts.Listen),
		fmt.Sprintf("TEST_TNT_MEMTX_USE_MVCC_ENGINE=%t", startOpts.MemtxUseMvccEngine),
		fmt.Sprintf("TEST_TNT_AUTH_TYPE=%s", startOpts.Auth),
	)

	// Copy SSL certificates.
	if startOpts.SslCertsDir != "" {
		err = copySslCerts(startOpts.WorkDir, startOpts.SslCertsDir)
		if err != nil {
			return inst, err
		}
	}

	// Options for restarting tarantool instance.
	inst.Opts = startOpts

	// Start tarantool.
	err = inst.Cmd.Start()
	if err != nil {
		return inst, err
	}

	// Try to connect and ping tarantool.
	// Using reconnect opts do not help on Connect,
	// see https://github.com/tarantool/go-tarantool/issues/136
	time.Sleep(startOpts.WaitStart)

	inst.checkDone()

	opts := tarantool.Opts{
		Timeout:    500 * time.Millisecond,
		SkipSchema: true,
	}

	var i int
	for i = 0; i <= startOpts.ConnectRetry; i++ {
		err = isReady(inst.Dialer, &opts)

		// Both connect and ping is ok.
		if err == nil {
			break
		}

		if i != startOpts.ConnectRetry {
			time.Sleep(startOpts.RetryTimeout)
		}
	}

	if inst.IsExit() && inst.result() != nil {
		StopTarantool(inst)
		return TarantoolInstance{}, fmt.Errorf("unexpected terminated Tarantool %q: %w",
			inst.Opts.Listen, inst.result())
	}

	if err != nil {
		StopTarantool(inst)
		return TarantoolInstance{}, fmt.Errorf("failed to connect Tarantool %q: %w",
			inst.Opts.Listen, err)
	}

	return inst, nil
}

// StopTarantool stops a tarantool instance started
// with StartTarantool. Waits until any resources
// associated with the process is released. If something went wrong, fails.
func StopTarantool(inst TarantoolInstance) {
	err := inst.Stop()
	if err != nil {
		log.Fatal(err)
	}
}

// StopTarantoolWithCleanup stops a tarantool instance started
// with StartTarantool. Waits until any resources
// associated with the process is released.
// Cleans work directory after stop. If something went wrong, fails.
func StopTarantoolWithCleanup(inst TarantoolInstance) {
	StopTarantool(inst)

	if inst.Opts.WorkDir != "" {
		if err := os.RemoveAll(inst.Opts.WorkDir); err != nil {
			log.Fatalf("Failed to clean work directory, got %s", err)
		}
	}
}

func copySslCerts(dst string, sslCertsDir string) (err error) {
	dstCertPath := filepath.Join(dst, sslCertsDir)
	if err = os.Mkdir(dstCertPath, 0755); err != nil {
		return
	}
	if err = copyDirectoryFiles(sslCertsDir, dstCertPath); err != nil {
		return
	}
	return
}

func copyDirectoryFiles(scrDir, dest string) error {
	entries, err := os.ReadDir(scrDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		sourcePath := filepath.Join(scrDir, entry.Name())
		destPath := filepath.Join(dest, entry.Name())
		_, err := os.Stat(sourcePath)
		if err != nil {
			return err
		}

		if err := copyFile(sourcePath, destPath); err != nil {
			return err
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		if err := os.Chmod(destPath, info.Mode()); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(srcFile, dstFile string) error {
	out, err := os.Create(dstFile)
	if err != nil {
		return err
	}

	defer out.Close()

	in, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer in.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	return nil
}

// msgpack.v5 decodes different uint types depending on value. The
// function helps to unify a result.
func ConvertUint64(v interface{}) (result uint64, err error) {
	switch v := v.(type) {
	case uint:
		result = uint64(v)
	case uint8:
		result = uint64(v)
	case uint16:
		result = uint64(v)
	case uint32:
		result = uint64(v)
	case uint64:
		result = v
	case int:
		result = uint64(v)
	case int8:
		result = uint64(v)
	case int16:
		result = uint64(v)
	case int32:
		result = uint64(v)
	case int64:
		result = uint64(v)
	default:
		err = fmt.Errorf("non-number value %T", v)
	}
	return
}

func GetConnectContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 500*time.Millisecond)
}
