package box_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2/box"
)

func TestNew(t *testing.T) {
	// Create a box instance with a nil connection. This should lead to a panic later.
	b := box.New(nil)

	// Ensure the box instance is not nil (which it shouldn't be), but this is not meaningful
	// since we will panic when we call the Info method with the nil connection.
	require.NotNil(t, b)

	// We expect a panic because we are passing a nil connection (nil Doer) to the By function.
	// The library does not control this zone, and the nil connection would cause a runtime error
	// when we attempt to call methods (like Info) on it.
	// This test ensures that such an invalid state is correctly handled by causing a panic,
	// as it's outside the library's responsibility.
	require.Panics(t, func() {

		// Calling Info on a box with a nil connection will result in a panic, since the underlying
		// connection (Doer) cannot perform the requested action (it's nil).
		_, _ = b.Info()
	})
}
