package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRole_String(t *testing.T) {
	require.Equal(t, "unknown", RoleUnknown.String())
	require.Equal(t, "master", RoleMaster.String())
	require.Equal(t, "replica", RoleReplica.String())
}
