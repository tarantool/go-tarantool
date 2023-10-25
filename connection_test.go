package tarantool_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"

	. "github.com/tarantool/go-tarantool/v2"
)

func TestOptsClonePreservesRequiredProtocolFeatures(t *testing.T) {
	original := Opts{
		RequiredProtocolInfo: ProtocolInfo{
			Version:  ProtocolVersion(100),
			Features: []iproto.Feature{iproto.Feature(99), iproto.Feature(100)},
		},
	}

	origCopy := original.Clone()

	original.RequiredProtocolInfo.Features[1] = iproto.Feature(98)

	require.Equal(t,
		origCopy,
		Opts{
			RequiredProtocolInfo: ProtocolInfo{
				Version:  ProtocolVersion(100),
				Features: []iproto.Feature{iproto.Feature(99), iproto.Feature(100)},
			},
		})
}
