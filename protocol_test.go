package tarantool_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/ice-blockchain/go-tarantool"
)

func TestProtocolInfoClonePreservesFeatures(t *testing.T) {
	original := ProtocolInfo{
		Version:  ProtocolVersion(100),
		Features: []ProtocolFeature{ProtocolFeature(99), ProtocolFeature(100)},
	}

	origCopy := original.Clone()

	original.Features[1] = ProtocolFeature(98)

	require.Equal(t,
		origCopy,
		ProtocolInfo{
			Version:  ProtocolVersion(100),
			Features: []ProtocolFeature{ProtocolFeature(99), ProtocolFeature(100)},
		})
}

func TestFeatureStringRepresentation(t *testing.T) {
	require.Equal(t, StreamsFeature.String(), "StreamsFeature")
	require.Equal(t, TransactionsFeature.String(), "TransactionsFeature")
	require.Equal(t, ErrorExtensionFeature.String(), "ErrorExtensionFeature")
	require.Equal(t, WatchersFeature.String(), "WatchersFeature")
	require.Equal(t, PaginationFeature.String(), "PaginationFeature")

	require.Equal(t, ProtocolFeature(15532).String(), "Unknown feature (code 15532)")
}
