package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRole_String(t *testing.T) {
	require.Equal(t, "unknown", UnknownRole.String())
	require.Equal(t, "master", MasterRole.String())
	require.Equal(t, "replica", ReplicaRole.String())
}
