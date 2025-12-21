package tarantool

import (
	"context"
	"log"
	"log/slog"
	"net"
)

// Logger интерфейс для пользовательских логгеров
type Logger interface {
	Report(event LogEvent, conn *Connection)
}

// SlogLogger логгер на основе slog
type SlogLogger struct {
	logger *slog.Logger
	ctx    context.Context
}

// NewSlogLogger создает новый логгер на основе slog
func NewSlogLogger(logger *slog.Logger) *SlogLogger {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogLogger{
		logger: logger,
		ctx:    context.Background(),
	}
}

// WithContext устанавливает контекст для логгера
func (l *SlogLogger) WithContext(ctx context.Context) *SlogLogger {
	return &SlogLogger{
		logger: l.logger,
		ctx:    ctx,
	}
}

func (l *SlogLogger) Report(event LogEvent, conn *Connection) {
	attrs := event.LogAttrs()

	if conn != nil {
		attrs = append(attrs,
			slog.String("connection_state", conn.stateToString()),
		)

		if conn.opts.MaxReconnects > 0 {
			attrs = append(attrs,
				slog.Uint64("max_reconnects", uint64(conn.opts.MaxReconnects)),
			)
		}
		if conn.opts.Reconnect > 0 {
			attrs = append(attrs,
				slog.String("reconnect_interval", conn.opts.Reconnect.String()),
			)
		}
		if conn.opts.Timeout > 0 {
			attrs = append(attrs,
				slog.String("request_timeout", conn.opts.Timeout.String()),
			)
		}
	}

	l.logger.LogAttrs(l.ctx, event.LogLevel(), event.EventName(), attrs...)
}

// SimpleLogger простой логгер на основе стандартного log
type SimpleLogger struct{}

func (l SimpleLogger) Report(event LogEvent, conn *Connection) {
	attrs := event.LogAttrs()

	var addrStr string
	if event, ok := event.(interface{ Addr() net.Addr }); ok && event.Addr() != nil {
		addrStr = event.Addr().String()
	}

	log.Printf("[%s] %s (addr: %s)", event.LogLevel(), event.EventName(), addrStr)

	for _, attr := range attrs {
		if attr.Key == "error" {
			log.Printf("  Error: %v", attr.Value.Any())
		} else if attr.Key == "request_id" {
			log.Printf("  Request ID: %v", attr.Value.Any())
		}
	}
}
