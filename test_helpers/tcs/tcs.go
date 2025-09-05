package tcs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

// ErrNotSupported identifies result of `Start()` why storage was not started.
var ErrNotSupported = errors.New("required Tarantool EE 3.3+")

// ErrNoValue used to show that `Get()` was successful, but no values were found.
var ErrNoValue = errors.New("required value not found")

// TCS is a Tarantool centralized configuration storage connection.
type TCS struct {
	inst *test_helpers.TarantoolInstance
	conn *tarantool.Connection
	tb   testing.TB
	port int
}

// dataResponse content of TcS response in data array.
type dataResponse struct {
	Path        string `msgpack:"path"`
	Value       string `msgpack:"value"`
	ModRevision int64  `msgpack:"mod_revision"`
}

// findEmptyPort returns some random unused port if @port is passed with zero.
func findEmptyPort(port int) (int, error) {
	listener, err := net.Listen("tcp4", fmt.Sprintf(":%d", port))
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

// Start starts a Tarantool centralized configuration storage.
// Use `port = 0` to use any unused port.
// Returns a Tcs instance and a cleanup function.
func Start(port int) (TCS, error) {
	tcs := TCS{}
	if ok, err := test_helpers.IsTcsSupported(); !ok || err != nil {
		return tcs, errors.Join(ErrNotSupported, err)
	}
	var err error
	tcs.port, err = findEmptyPort(port)
	if err != nil {
		if port == 0 {
			return tcs, fmt.Errorf("failed to detect an empty port: %w", err)
		} else {
			return tcs, fmt.Errorf("port %d can't be used: %w", port, err)
		}
	}

	opts, err := makeOpts(tcs.port)
	if err != nil {
		return tcs, err
	}

	tcs.inst, err = test_helpers.StartTarantool(opts)
	if err != nil {
		return tcs, fmt.Errorf("failed to start Tarantool config storage: %w", err)
	}

	tcs.conn, err = tarantool.Connect(context.Background(), tcs.inst.Dialer, tarantool.Opts{})
	if err != nil {
		return tcs, fmt.Errorf("failed to connect to Tarantool config storage: %w", err)
	}

	return tcs, nil
}

// Start starts a Tarantool centralized configuration storage.
// Returns a Tcs instance and a cleanup function.
func StartTesting(tb testing.TB, port int) TCS {
	tcs, err := Start(port)
	if err != nil {
		tb.Fatal(err)
	}
	return tcs
}

// Doer returns interface for interacting with Tarantool.
func (t *TCS) Doer() tarantool.Doer {
	return t.conn
}

// Dialer returns a dialer to connect to Tarantool.
func (t *TCS) Dialer() tarantool.Dialer {
	return t.inst.Dialer
}

// Endpoints returns a list of addresses to connect.
func (t *TCS) Endpoints() []string {
	return []string{fmt.Sprintf("127.0.0.1:%d", t.port)}
}

// Credentials returns a user name and password to connect.
func (t *TCS) Credentials() (string, string) {
	return tcsUser, tcsPassword
}

// Stop stops the Tarantool centralized configuration storage.
func (t *TCS) Stop() {
	if t.tb != nil {
		t.tb.Helper()
	}
	if t.conn != nil {
		t.conn.Close()
	}
	test_helpers.StopTarantoolWithCleanup(t.inst)
}

// Put implements "config.storage.put" method.
func (t *TCS) Put(ctx context.Context, path string, value string) error {
	if t.tb != nil {
		t.tb.Helper()
	}
	req := tarantool.NewCallRequest("config.storage.put").
		Args([]any{path, value}).
		Context(ctx)
	if _, err := t.conn.Do(req).GetResponse(); err != nil {
		return fmt.Errorf("failed to save data to tarantool: %w", err)
	}
	return nil
}

// Delete implements "config.storage.delete" method.
func (t *TCS) Delete(ctx context.Context, path string) error {
	if t.tb != nil {
		t.tb.Helper()
	}
	req := tarantool.NewCallRequest("config.storage.delete").
		Args([]any{path}).
		Context(ctx)
	if _, err := t.conn.Do(req).GetResponse(); err != nil {
		return fmt.Errorf("failed to delete data from tarantool: %w", err)
	}
	return nil
}

// Get implements "config.storage.get" method.
func (t *TCS) Get(ctx context.Context, path string) (string, error) {
	if t.tb != nil {
		t.tb.Helper()
	}
	req := tarantool.NewCallRequest("config.storage.get").
		Args([]any{path}).
		Context(ctx)

	resp := []struct {
		Data []dataResponse `msgpack:"data"`
	}{}

	err := t.conn.Do(req).GetTyped(&resp)
	if err != nil {
		return "", fmt.Errorf("failed to fetch data from tarantool: %w", err)
	}
	if len(resp) != 1 {
		return "", errors.New("unexpected response from tarantool")
	}
	if len(resp[0].Data) == 0 {
		return "", ErrNoValue
	}
	if len(resp[0].Data) != 1 {
		return "", errors.New("too much data in response from tarantool")
	}

	return resp[0].Data[0].Value, nil
}
