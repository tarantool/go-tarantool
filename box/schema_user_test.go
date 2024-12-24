package box

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

func TestNewSchemaUser(t *testing.T) {
	// Create a schema user instance with a nil connection. This should lead to a panic later.
	su := NewSchemaUser(nil)

	// Ensure the schema user is not nil (which it shouldn't be), but this is not meaningful
	// since we will panic when we call some method with the nil connection.
	require.NotNil(t, su)

	// We expect a panic because we are passing a nil connection (nil Doer) to the By function.
	// The library does not control this zone, and the nil connection would cause a runtime error
	// when we attempt to call methods (like Info) on it.
	// This test ensures that such an invalid state is correctly handled by causing a panic,
	// as it's outside the library's responsibility.
	require.Panics(t, func() {
		// Calling Exists on a schema user with a nil connection will result in a panic,
		// since the underlying connection (Doer) cannot perform the requested action (it's nil).
		_, _ = su.Exists(context.TODO(), "test")
	})
}

func TestUserExistsResponse_DecodeMsgpack(t *testing.T) {
	tCases := map[bool]func() *bytes.Buffer{
		true: func() *bytes.Buffer {
			buf := bytes.NewBuffer(nil)
			buf.WriteByte(msgpcode.FixedArrayLow | byte(1))
			buf.WriteByte(msgpcode.True)

			return buf
		},
		false: func() *bytes.Buffer {
			buf := bytes.NewBuffer(nil)
			buf.WriteByte(msgpcode.FixedArrayLow | byte(1))
			buf.WriteByte(msgpcode.False)

			return buf
		},
	}

	for tCaseBool, tCaseBuf := range tCases {
		tCaseBool := tCaseBool
		tCaseBuf := tCaseBuf()

		t.Run(fmt.Sprintf("case: %t", tCaseBool), func(t *testing.T) {
			t.Parallel()

			resp := UserExistsResponse{}

			require.NoError(t, resp.DecodeMsgpack(msgpack.NewDecoder(tCaseBuf)))
			require.Equal(t, tCaseBool, resp.Exists)
		})
	}

}

func TestUserPasswordResponse_DecodeMsgpack(t *testing.T) {
	tCases := []string{
		"test",
		"$tr0ng_pass",
	}

	for _, tCase := range tCases {
		tCase := tCase

		t.Run(tCase, func(t *testing.T) {
			t.Parallel()
			buf := bytes.NewBuffer(nil)
			buf.WriteByte(msgpcode.FixedArrayLow | byte(1))

			bts, err := msgpack.Marshal(tCase)
			require.NoError(t, err)
			buf.Write(bts)

			resp := UserPasswordResponse{}

			err = resp.DecodeMsgpack(msgpack.NewDecoder(buf))
			require.NoError(t, err)
			require.Equal(t, tCase, resp.Hash)
		})
	}

}

func FuzzUserPasswordResponse_DecodeMsgpack(f *testing.F) {
	f.Fuzz(func(t *testing.T, orig string) {
		buf := bytes.NewBuffer(nil)
		buf.WriteByte(msgpcode.FixedArrayLow | byte(1))

		bts, err := msgpack.Marshal(orig)
		require.NoError(t, err)
		buf.Write(bts)

		resp := UserPasswordResponse{}

		err = resp.DecodeMsgpack(msgpack.NewDecoder(buf))
		require.NoError(t, err)
		require.Equal(t, orig, resp.Hash)
	})
}

func TestNewUserExistsRequest(t *testing.T) {
	t.Parallel()

	req := UserExistsRequest{}

	require.NotPanics(t, func() {
		req = NewUserExistsRequest("test")
	})

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserCreateRequest(t *testing.T) {
	t.Parallel()

	req := UserCreateRequest{}

	require.NotPanics(t, func() {
		req = NewUserCreateRequest("test", UserCreateOptions{})
	})

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserDropRequest(t *testing.T) {
	t.Parallel()

	req := UserDropRequest{}

	require.NotPanics(t, func() {
		req = NewUserDropRequest("test", UserDropOptions{})
	})

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserPasswordRequest(t *testing.T) {
	t.Parallel()

	req := UserPasswordRequest{}

	require.NotPanics(t, func() {
		req = NewUserPasswordRequest("test")
	})

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserPasswdRequest(t *testing.T) {
	t.Parallel()

	var err error
	req := UserPasswdRequest{}

	require.NotPanics(t, func() {
		req, err = NewUserPasswdRequest("test")
		require.NoError(t, err)
	})

	_, err = NewUserPasswdRequest()
	require.Errorf(t, err, "invalid arguments count")

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserInfoRequest(t *testing.T) {
	t.Parallel()

	var err error
	req := UserInfoRequest{}

	require.NotPanics(t, func() {
		req = NewUserInfoRequest("test")
		require.NoError(t, err)
	})

	require.Implements(t, (*tarantool.Request)(nil), req)
}
