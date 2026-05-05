package tarantool

import (
	"errors"
	"net"
	"testing"

	"github.com/tarantool/go-iproto"
)

func TestClientError_IsSentinel(t *testing.T) {
	cases := []struct {
		code     ClientErrorCode
		sentinel error
	}{
		{CodeConnectionNotReady, ErrConnectionNotReady},
		{CodeConnectionClosed, ErrConnectionClosed},
		{CodeProtocolError, ErrProtocolError},
		{CodeTimeouted, ErrTimeouted},
		{CodeRateLimited, ErrRateLimited},
		{CodeConnectionShutdown, ErrConnectionShutdown},
		{CodeIoError, ErrIoError},
	}
	for _, tc := range cases {
		err := newClientError(tc.code, "x", nil)
		if !errors.Is(err, tc.sentinel) {
			t.Errorf("code 0x%x: errors.Is did not match its sentinel", uint32(tc.code))
		}
	}
}

func TestClientError_IsRetryable(t *testing.T) {
	retryable := []ClientErrorCode{
		CodeConnectionNotReady, CodeTimeouted, CodeRateLimited, CodeIoError,
	}
	notRetryable := []ClientErrorCode{
		CodeConnectionClosed, CodeProtocolError, CodeConnectionShutdown,
	}
	for _, code := range retryable {
		err := newClientError(code, "x", nil)
		if !IsRetryable(err) {
			t.Errorf("code 0x%x should be retryable", uint32(code))
		}
		if !errors.Is(err, ErrRetryable) {
			t.Errorf("code 0x%x: errors.Is(_, ErrRetryable) failed", uint32(code))
		}
	}
	for _, code := range notRetryable {
		err := newClientError(code, "x", nil)
		if IsRetryable(err) {
			t.Errorf("code 0x%x should NOT be retryable", uint32(code))
		}
	}
}

func TestClientError_WrapsCause(t *testing.T) {
	cause := &net.OpError{Op: "read", Err: errors.New("eof")}
	err := newClientError(CodeIoError, "failed to read", cause)

	var got *net.OpError
	if !errors.As(err, &got) {
		t.Fatal("errors.As did not unwrap to *net.OpError")
	}
	if got != cause {
		t.Fatal("errors.As returned a different *net.OpError instance")
	}
	if !errors.Is(err, ErrIoError) {
		t.Fatal("errors.Is did not match ErrIoError after wrap")
	}
	if !errors.Is(err, ErrRetryable) {
		t.Fatal("I/O error should also imply ErrRetryable")
	}
}

func TestClientError_ErrorString(t *testing.T) {
	cases := []struct {
		err  ClientError
		want string
	}{
		{
			newClientError(CodeConnectionShutdown, "server shutdown in progress", nil),
			"connection shutdown: server shutdown in progress",
		},
		{
			newClientError(CodeIoError, "failed to read", errors.New("eof")),
			"I/O error: failed to read: eof",
		},
		{
			newClientError(CodeTimeouted, "", nil),
			"request timed out",
		},
		{
			ClientError{Code: ClientErrorCode(0x9999), Msg: "weird"},
			"client error 0x9999: weird",
		},
	}
	for _, tc := range cases {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("Error() = %q, want %q", got, tc.want)
		}
	}
}

func TestServerError_AsAndFormat(t *testing.T) {
	se := ServerError{Code: iproto.ER_TUPLE_FOUND, Msg: "Duplicate key exists"}

	var got ServerError
	if !errors.As(se, &got) {
		t.Fatal("errors.As did not match ServerError")
	}
	if got.Code != iproto.ER_TUPLE_FOUND {
		t.Fatalf("Code = %v", got.Code)
	}
	if want := "Duplicate key exists (0x3)"; se.Error() != want {
		t.Errorf("Error() = %q, want %q", se.Error(), want)
	}
}

func TestSentinelError_DoesNotMatchOtherSentinel(t *testing.T) {
	err := newClientError(CodeTimeouted, "x", nil)
	if errors.Is(err, ErrConnectionClosed) {
		t.Error("ErrTimeouted matched ErrConnectionClosed")
	}
}
