package tarantool

import (
	"context"
	"io"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"
)

// BroadcastRequest helps to send broadcast messages. See:
// https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_events/broadcast/
type BroadcastRequest struct {
	call *CallRequest
	key  string
}

// NewBroadcastRequest returns a new broadcast request for a specified key.
func NewBroadcastRequest(key string) *BroadcastRequest {
	req := new(BroadcastRequest)
	req.key = key
	req.call = NewCallRequest("box.broadcast").Args([]interface{}{key})
	return req
}

// Value sets the value for the broadcast request.
// Note: default value is nil.
func (req *BroadcastRequest) Value(value interface{}) *BroadcastRequest {
	req.call = req.call.Args([]interface{}{req.key, value})
	return req
}

// Context sets a passed context to the broadcast request.
func (req *BroadcastRequest) Context(ctx context.Context) *BroadcastRequest {
	req.call = req.call.Context(ctx)
	return req
}

// Code returns IPROTO code for the broadcast request.
func (req *BroadcastRequest) Type() iproto.Type {
	return req.call.Type()
}

// Body fills an msgpack.Encoder with the broadcast request body.
func (req *BroadcastRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	return req.call.Body(res, enc)
}

// Ctx returns a context of the broadcast request.
func (req *BroadcastRequest) Ctx() context.Context {
	return req.call.Ctx()
}

// Async returns is the broadcast request expects a response.
func (req *BroadcastRequest) Async() bool {
	return req.call.Async()
}

// Response creates a response for a BroadcastRequest.
func (req *BroadcastRequest) Response(header Header, body io.Reader) (Response, error) {
	resp, err := createBaseResponse(header, body)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// watchRequest subscribes to the updates of a specified key defined on the
// server. After receiving the notification, you should send a new
// watchRequest to acknowledge the notification.
type watchRequest struct {
	baseRequest
	key string
	ctx context.Context
}

// newWatchRequest returns a new watchRequest.
func newWatchRequest(key string) *watchRequest {
	req := new(watchRequest)
	req.rtype = iproto.IPROTO_WATCH
	req.async = true
	req.key = key
	return req
}

// Body fills an msgpack.Encoder with the watch request body.
func (req *watchRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(1); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(iproto.IPROTO_EVENT_KEY)); err != nil {
		return err
	}
	return enc.EncodeString(req.key)
}

// Context sets a passed context to the request.
func (req *watchRequest) Context(ctx context.Context) *watchRequest {
	req.ctx = ctx
	return req
}

// unwatchRequest unregisters a watcher subscribed to the given notification
// key.
type unwatchRequest struct {
	baseRequest
	key string
	ctx context.Context
}

// newUnwatchRequest returns a new unwatchRequest.
func newUnwatchRequest(key string) *unwatchRequest {
	req := new(unwatchRequest)
	req.rtype = iproto.IPROTO_UNWATCH
	req.async = true
	req.key = key
	return req
}

// Body fills an msgpack.Encoder with the unwatch request body.
func (req *unwatchRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(1); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(iproto.IPROTO_EVENT_KEY)); err != nil {
		return err
	}
	return enc.EncodeString(req.key)
}

// Context sets a passed context to the request.
func (req *unwatchRequest) Context(ctx context.Context) *unwatchRequest {
	req.ctx = ctx
	return req
}

// WatchEvent is a watch notification event received from a server.
type WatchEvent struct {
	Conn  *Connection // A source connection.
	Key   string      // A key.
	Value interface{} // A value.
}

// Watcher is a subscription to broadcast events.
type Watcher interface {
	// Unregister unregisters the watcher.
	Unregister()
}

// WatchCallback is a callback to invoke when the key value is updated.
type WatchCallback func(event WatchEvent)
