package tarantool

import (
	"context"
	"fmt"
)

// ProtocolVersion type stores Tarantool protocol version.
type ProtocolVersion uint64

// ProtocolVersion type stores a Tarantool protocol feature.
type ProtocolFeature uint64

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
	Version: ProtocolVersion(4),
	// Streams and transactions were introduced in protocol version 1
	// (Tarantool 2.10.0), in connector since 1.7.0.
	// Error extension type was introduced in protocol
	// version 2 (Tarantool 2.10.0), in connector since 1.10.0.
	// Watchers were introduced in protocol version 3 (Tarantool 2.10.0), in
	// connector since 1.10.0.
	// Pagination were introduced in protocol version 4 (Tarantool 2.11.0), in
	// connector since 1.11.0.
	Features: []ProtocolFeature{
		StreamsFeature,
		TransactionsFeature,
		ErrorExtensionFeature,
		WatchersFeature,
		PaginationFeature,
	},
}

// IdRequest informs the server about supported protocol
// version and protocol features.
type IdRequest struct {
	baseRequest
	protocolInfo ProtocolInfo
}

func fillId(enc *encoder, protocolInfo ProtocolInfo) error {
	enc.EncodeMapLen(2)

	encodeUint(enc, KeyVersion)
	if err := enc.Encode(protocolInfo.Version); err != nil {
		return err
	}

	encodeUint(enc, KeyFeatures)

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
	req.requestCode = IdRequestCode
	req.protocolInfo = protocolInfo.Clone()
	return req
}

// Body fills an encoder with the id request body.
func (req *IdRequest) Body(res SchemaResolver, enc *encoder) error {
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
