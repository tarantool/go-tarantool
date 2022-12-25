package tarantool_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/tarantool/go-tarantool/v2"
)

func TestOptsClonePreservesRequiredProtocolFeatures(t *testing.T) {
	original := Opts{
		RequiredProtocolInfo: ProtocolInfo{
			Version:  ProtocolVersion(100),
			Features: []ProtocolFeature{ProtocolFeature(99), ProtocolFeature(100)},
		},
	}

	origCopy := original.Clone()

	original.RequiredProtocolInfo.Features[1] = ProtocolFeature(98)

	require.Equal(t,
		origCopy,
		Opts{
			RequiredProtocolInfo: ProtocolInfo{
				Version:  ProtocolVersion(100),
				Features: []ProtocolFeature{ProtocolFeature(99), ProtocolFeature(100)},
			},
		})
}
