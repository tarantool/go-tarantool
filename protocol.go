package tarantool

import (
	"context"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"
)

// ProtocolVersion type stores Tarantool protocol version.
type ProtocolVersion uint64

// ProtocolInfo type aggregates Tarantool protocol version and features info.
type ProtocolInfo struct {
	// Auth is an authentication method.
	Auth Auth
	// Version is the supported protocol version.
	Version ProtocolVersion
	// Features are supported protocol features.
	Features []iproto.Feature
}

// Clone returns an exact copy of the ProtocolInfo object.
// Any changes in copy will not affect the original values.
func (info ProtocolInfo) Clone() ProtocolInfo {
	infoCopy := info

	if info.Features != nil {
		infoCopy.Features = make([]iproto.Feature, len(info.Features))
		copy(infoCopy.Features, info.Features)
	}

	return infoCopy
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
	Features: []iproto.Feature{
		iproto.IPROTO_FEATURE_STREAMS,
		iproto.IPROTO_FEATURE_TRANSACTIONS,
		iproto.IPROTO_FEATURE_ERROR_EXTENSION,
		iproto.IPROTO_FEATURE_WATCHERS,
		iproto.IPROTO_FEATURE_PAGINATION,
		iproto.IPROTO_FEATURE_SPACE_AND_INDEX_NAMES,
		iproto.IPROTO_FEATURE_WATCH_ONCE,
	},
}

// IdRequest informs the server about supported protocol
// version and protocol features.
type IdRequest struct {
	baseRequest
	protocolInfo ProtocolInfo
}

// NewIdRequest returns a new IdRequest.
func NewIdRequest(protocolInfo ProtocolInfo) *IdRequest {
	req := new(IdRequest)
	req.rtype = iproto.IPROTO_ID
	req.protocolInfo = protocolInfo.Clone()
	return req
}

// Body fills an msgpack.Encoder with the id request body.
func (req *IdRequest) Body(_ SchemaResolver, enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_VERSION)); err != nil {
		return err
	}

	if err := enc.Encode(req.protocolInfo.Version); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_FEATURES)); err != nil {
		return err
	}

	if err := enc.EncodeArrayLen(len(req.protocolInfo.Features)); err != nil {
		return err
	}

	for _, feature := range req.protocolInfo.Features {
		if err := enc.Encode(feature); err != nil {
			return err
		}
	}

	return nil
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
