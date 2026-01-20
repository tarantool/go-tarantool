package tarantool

import (
	"log/slog"
	"net"
	"time"
)

type LogEvent interface {
	EventName() string
	LogLevel() slog.Level
	LogAttrs() []slog.Attr
}

type baseEvent struct {
	addr net.Addr
	time time.Time
}

func newBaseEvent(addr net.Addr) baseEvent {
	return baseEvent{
		addr: addr,
		time: time.Now(),
	}
}

func (e baseEvent) baseAttrs() []slog.Attr {
	attrs := []slog.Attr{
		slog.String("component", "tarantool.connection"),
		slog.Time("time", e.time),
	}
	if e.addr != nil {
		attrs = append(attrs, slog.String("addr", e.addr.String()))
	}
	return attrs
}

type ReconnectFailedEvent struct {
	baseEvent
	Reconnects    uint
	MaxReconnects uint
	Error         error
	IsInitial     bool
}

func (e ReconnectFailedEvent) EventName() string    { return "reconnect_failed" }
func (e ReconnectFailedEvent) LogLevel() slog.Level { return slog.LevelError }
func (e ReconnectFailedEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	attrs = append(attrs,
		slog.String("event", e.EventName()),
		slog.Uint64("reconnects", uint64(e.Reconnects)),
		slog.Uint64("max_reconnects", uint64(e.MaxReconnects)),
		slog.String("error", e.Error.Error()),
		slog.Bool("is_initial", e.IsInitial),
	)
	return attrs
}

type LastReconnectFailedEvent struct {
	baseEvent
	Error error
}

func (e LastReconnectFailedEvent) EventName() string    { return "last_reconnect_failed" }
func (e LastReconnectFailedEvent) LogLevel() slog.Level { return slog.LevelError }
func (e LastReconnectFailedEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	attrs = append(attrs,
		slog.String("event", e.EventName()),
		slog.String("error", e.Error.Error()),
	)
	return attrs
}

type UnexpectedResultIdEvent struct {
	baseEvent
	RequestId uint32
}

func (e UnexpectedResultIdEvent) EventName() string    { return "unexpected_result_id" }
func (e UnexpectedResultIdEvent) LogLevel() slog.Level { return slog.LevelWarn }
func (e UnexpectedResultIdEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	attrs = append(attrs,
		slog.String("event", e.EventName()),
		slog.Uint64("request_id", uint64(e.RequestId)),
	)
	return attrs
}

type WatchEventReadFailedEvent struct {
	baseEvent
	Error error
}

func (e WatchEventReadFailedEvent) EventName() string    { return "watch_event_read_failed" }
func (e WatchEventReadFailedEvent) LogLevel() slog.Level { return slog.LevelWarn }
func (e WatchEventReadFailedEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	attrs = append(attrs,
		slog.String("event", e.EventName()),
		slog.String("error", e.Error.Error()),
	)
	return attrs
}

type BoxSessionPushUnsupportedEvent struct {
	baseEvent
	RequestId uint32
}

func (e BoxSessionPushUnsupportedEvent) EventName() string    { return "box_session_push_unsupported" }
func (e BoxSessionPushUnsupportedEvent) LogLevel() slog.Level { return slog.LevelWarn }
func (e BoxSessionPushUnsupportedEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	attrs = append(attrs,
		slog.String("event", e.EventName()),
		slog.Uint64("request_id", uint64(e.RequestId)),
	)
	return attrs
}

type ConnectedEvent struct {
	baseEvent
	Reconnects uint
}

func (e ConnectedEvent) EventName() string    { return "connected" }
func (e ConnectedEvent) LogLevel() slog.Level { return slog.LevelInfo }
func (e ConnectedEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	attrs = append(attrs,
		slog.String("event", e.EventName()),
		slog.Uint64("reconnects", uint64(e.Reconnects)),
	)
	return attrs
}

type DisconnectedEvent struct {
	baseEvent
	Reason error
}

func (e DisconnectedEvent) EventName() string    { return "disconnected" }
func (e DisconnectedEvent) LogLevel() slog.Level { return slog.LevelWarn }
func (e DisconnectedEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	if e.Reason != nil {
		attrs = append(attrs, slog.String("reason", e.Reason.Error()))
	}
	return attrs
}

type ShutdownEvent struct {
	baseEvent
}

func (e ShutdownEvent) EventName() string    { return "shutdown" }
func (e ShutdownEvent) LogLevel() slog.Level { return slog.LevelInfo }
func (e ShutdownEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	attrs = append(attrs, slog.String("event", e.EventName()))
	return attrs
}

type ClosedEvent struct {
	baseEvent
}

func (e ClosedEvent) EventName() string    { return "closed" }
func (e ClosedEvent) LogLevel() slog.Level { return slog.LevelInfo }
func (e ClosedEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	attrs = append(attrs, slog.String("event", e.EventName()))
	return attrs
}

type ConnectionPoolEvent struct {
	baseEvent
	PoolSize    int
	ActiveConns int
	Event       string
}

func (e ConnectionPoolEvent) EventName() string    { return "connection_pool_" + e.Event }
func (e ConnectionPoolEvent) LogLevel() slog.Level { return slog.LevelInfo }
func (e ConnectionPoolEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	attrs = append(attrs,
		slog.String("event", e.EventName()),
		slog.Int("pool_size", e.PoolSize),
		slog.Int("active_connections", e.ActiveConns),
		slog.String("pool_event", e.Event),
	)
	return attrs
}

type TimeoutEvent struct {
	baseEvent
	RequestId uint32
	Timeout   time.Duration
}

func (e TimeoutEvent) EventName() string    { return "timeout" }
func (e TimeoutEvent) LogLevel() slog.Level { return slog.LevelWarn }
func (e TimeoutEvent) LogAttrs() []slog.Attr {
	attrs := e.baseAttrs()
	attrs = append(attrs,
		slog.String("event", e.EventName()),
		slog.Uint64("request_id", uint64(e.RequestId)),
		slog.String("timeout", e.Timeout.String()),
	)
	return attrs
}
