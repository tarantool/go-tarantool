package box

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBox_Session(t *testing.T) {
	b := New(nil)
	require.NotNil(t, b.Session())
}

func TestNewSession(t *testing.T) {
	require.NotPanics(t, func() {
		NewSession(nil)
	})
}

func TestNewSessionSuRequest(t *testing.T) {
	_, err := NewSessionSuRequest("admin", 1, 2, 3)
	require.Error(t, err, "error should be occurred, because of tarantool signature requires")

	_, err = NewSessionSuRequest("admin")
	require.NoError(t, err)
}
