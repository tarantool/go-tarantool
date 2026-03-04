package tarantool

import (
	"context"
	"log/slog"
	"sync/atomic"
)

type Logger interface {
	Report(event LogEvent, conn *Connection)
}

type SlogLogger struct {
	logger *slog.Logger
	ctx    context.Context
}

// NewSlogLogger creates a new SlogLogger that uses the provided slog.Logger.
// If logger is nil, it uses slog.Default().
// Note: slog.Default() logs at Info level by default. To log events with lower levels
// (e.g., Debug), either configure the default logger's level using slog.SetLogLoggerLevel
// or provide a custom logger with the desired level.
func NewSlogLogger(logger *slog.Logger) SlogLogger {
	if logger == nil {
		logger = slog.Default()
	}
	return SlogLogger{
		logger: logger,
		ctx:    context.Background(),
	}
}

func (l *SlogLogger) WithContext(ctx context.Context) SlogLogger {
	return SlogLogger{
		logger: l.logger,
		ctx:    ctx,
	}
}

func (l SlogLogger) Report(event LogEvent, conn *Connection) {
	attrs := event.LogAttrs()

	if conn != nil {
		keys := make(map[string]bool, len(attrs))
		for _, a := range attrs {
			keys[a.Key] = true
		}

		if !keys["connection_state"] {
			state := ConnectionState(atomic.LoadUint32(&conn.state))
			attrs = append(attrs, slog.String("connection_state", state.String()))
		}

		if conn.opts.MaxReconnects > 0 && !keys["max_reconnects"] {
			attrs = append(attrs, slog.Uint64("max_reconnects", uint64(conn.opts.MaxReconnects)))
		}
		if conn.opts.Reconnect > 0 && !keys["reconnect_interval"] {
			attrs = append(attrs, slog.String("reconnect_interval", conn.opts.Reconnect.String()))
		}
		if conn.opts.Timeout > 0 && !keys["request_timeout"] {
			attrs = append(attrs, slog.String("request_timeout", conn.opts.Timeout.String()))
		}
	}

	l.logger.LogAttrs(l.ctx, event.LogLevel(), event.Message(), attrs...)
}
