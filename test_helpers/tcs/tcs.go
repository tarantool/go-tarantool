package tcs

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var ErrNotSupported = errors.New("required Tarantool EE 3.3+")

// TCS is a Tarantool centralized configuration storage connection.
type TCS struct {
	inst test_helpers.TarantoolInstance
	conn *tarantool.Connection
	tb   testing.TB
}

// getResponse is a response of a get request.
type getResponse struct {
	Data []struct {
		Path        string
		Value       string
		ModRevision int64 `mapstructure:"mod_revision"`
	}
}

func isSupported() bool {
	if less, err := test_helpers.IsTarantoolEE(); less || err != nil {
		return false
	}
	if less, err := test_helpers.IsTarantoolVersionLess(3, 3, 0); less || err != nil {
		return false
	}
	return true

}

// Start starts a Tarantool centralized configuration storage.
// Returns a Tcs instance and a cleanup function.
func Start(port int) (TCS, error) {
	tcs := TCS{}
	if !isSupported() {
		return tcs, ErrNotSupported
	}
	opts, err := makeOpts(port)
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

// Doer return interface for interacting with Tarantool.
func (t *TCS) Doer() tarantool.Doer {
	return t.conn
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
		return fmt.Errorf("failed save data to tarantool: %w", err)
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
		return fmt.Errorf("failed delete data from tarantool: %w", err)
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

	resp := []getResponse{}
	err := t.conn.Do(req).GetTyped(&resp)
	if err != nil {
		return "", fmt.Errorf("failed to fetch data from tarantool: %w", err)
	}

	if len(resp) != 1 {
		return "", errors.New("unexpected response from tarantool")
	}

	return resp[0].Data[0].Value, nil
}
