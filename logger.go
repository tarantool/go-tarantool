package tarantool

import (
	"context"
	"log"
	"log/slog"
)

type Logger interface {
	Report(event LogEvent, conn *Connection)
}

type SlogLogger struct {
	logger *slog.Logger
	ctx    context.Context
}

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
			attrs = append(attrs, slog.String("connection_state", conn.stateToString()))
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

type SimpleLogger struct{}

func (l SimpleLogger) Report(event LogEvent, conn *Connection) {
	attrs := event.LogAttrs()

	log.Printf("[%s] %s [event=%s]", event.LogLevel(), event.Message(), event.EventName())

	for _, attr := range attrs {
		if attr.Key == "error" {
			log.Printf("  Error: %v", attr.Value.Any())
		} else if attr.Key == "request_id" {
			log.Printf("  Request ID: %v", attr.Value.Any())
		}
	}
}
