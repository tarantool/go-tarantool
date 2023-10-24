package tarantool

import (
	"context"
	"fmt"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"
)

// ProtocolVersion type stores Tarantool protocol version.
type ProtocolVersion uint64

// ProtocolVersion type stores a Tarantool protocol feature.
type ProtocolFeature iproto.Feature

// ProtocolInfo type aggregates Tarantool protocol version and features info.
type ProtocolInfo struct {
	// Auth is an authentication method.
	Auth Auth
	// Version is the supported protocol version.
	Version ProtocolVersion
	// Features are supported protocol features.
	Features []ProtocolFeature
}

// Clone returns an exact copy of the ProtocolInfo object.
// Any changes in copy will not affect the original values.
func (info ProtocolInfo) Clone() ProtocolInfo {
	infoCopy := info

	if info.Features != nil {
		infoCopy.Features = make([]ProtocolFeature, len(info.Features))
		copy(infoCopy.Features, info.Features)
	}

	return infoCopy
}

const (
	// StreamsFeature represents streams support (supported by connector).
	StreamsFeature ProtocolFeature = 0
	// TransactionsFeature represents interactive transactions support.
	// (supported by connector).
	TransactionsFeature ProtocolFeature = 1
	// ErrorExtensionFeature represents support of MP_ERROR objects over MessagePack
	// (supported by connector).
	ErrorExtensionFeature ProtocolFeature = 2
	// WatchersFeature represents support of watchers
	// (supported by connector).
	WatchersFeature ProtocolFeature = 3
	// PaginationFeature represents support of pagination
	// (supported by connector).
	PaginationFeature ProtocolFeature = 4
	// WatchOnceFeature represents support of WatchOnce request types.
	WatchOnceFeature ProtocolFeature = 6
)

// String returns the name of a Tarantool feature.
// If value X is not a known feature, returns "Unknown feature (code X)" string.
func (ftr ProtocolFeature) String() string {
	switch ftr {
	case StreamsFeature:
		return "StreamsFeature"
	case TransactionsFeature:
		return "TransactionsFeature"
	case ErrorExtensionFeature:
		return "ErrorExtensionFeature"
	case WatchersFeature:
		return "WatchersFeature"
	case PaginationFeature:
		return "PaginationFeature"
	case WatchOnceFeature:
		return "WatchOnceFeature"
	default:
		return fmt.Sprintf("Unknown feature (code %d)", ftr)
	}
}

var clientProtocolInfo ProtocolInfo = ProtocolInfo{
	// Protocol version supported by connector. Version 3
	// was introduced in Tarantool 2.10.0, version 4 was
	// introduced in master 948e5cd (possible 2.10.5 or 2.11.0).
	// Support of protocol version on connector side was introduced in
	// 1.10.0.
	Version: ProtocolVersion(6),
	// Streams and transactions were introduced in protocol version 1
	// (Tarantool 2.10.0), in connector since 1.7.0.
	// Error extension type was introduced in protocol
	// version 2 (Tarantool 2.10.0), in connector since 1.10.0.
	// Watchers were introduced in protocol version 3 (Tarantool 2.10.0), in
	// connector since 1.10.0.
	// Pagination were introduced in protocol version 4 (Tarantool 2.11.0), in
	// connector since 1.11.0.
	// WatchOnce request type was introduces in protocol version 6
	// (Tarantool 3.0.0), in connector since 2.0.0.
	Features: []ProtocolFeature{
		StreamsFeature,
		TransactionsFeature,
		ErrorExtensionFeature,
		WatchersFeature,
		PaginationFeature,
		WatchOnceFeature,
	},
}

// IdRequest informs the server about supported protocol
// version and protocol features.
type IdRequest struct {
	baseRequest
	protocolInfo ProtocolInfo
}

func fillId(enc *msgpack.Encoder, protocolInfo ProtocolInfo) error {
	enc.EncodeMapLen(2)

	enc.EncodeUint(uint64(iproto.IPROTO_VERSION))
	if err := enc.Encode(protocolInfo.Version); err != nil {
		return err
	}

	enc.EncodeUint(uint64(iproto.IPROTO_FEATURES))

	t := len(protocolInfo.Features)
	if err := enc.EncodeArrayLen(t); err != nil {
		return err
	}

	for _, feature := range protocolInfo.Features {
		if err := enc.Encode(feature); err != nil {
			return err
		}
	}

	return nil
}

// NewIdRequest returns a new IdRequest.
func NewIdRequest(protocolInfo ProtocolInfo) *IdRequest {
	req := new(IdRequest)
	req.rtype = iproto.IPROTO_ID
	req.protocolInfo = protocolInfo.Clone()
	return req
}

// Body fills an msgpack.Encoder with the id request body.
func (req *IdRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	return fillId(enc, req.protocolInfo)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *IdRequest) Context(ctx context.Context) *IdRequest {
	req.ctx = ctx
	return req
}
