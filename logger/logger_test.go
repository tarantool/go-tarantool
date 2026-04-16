package logger_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/logger"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

func ExampleNewSlogLogger() {
	var buf bytes.Buffer

	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slogLogger := slog.New(handler)

	opts := tarantool.Opts{
		Logger:  logger.NewSlogLogger(slogLogger),
		Timeout: 5 * time.Second,
	}

	ctx := context.Background()
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3018",
		User:     "test",
		Password: "test",
	}
	conn, err := tarantool.Connect(ctx, dialer, opts)
	if err != nil {
		slog.Error("failed to connect", "error", err)
		return
	}
	defer func() { _ = conn.Close() }()

	_, _ = conn.Do(tarantool.NewPingRequest()).Get()

	output := buf.String()

	reTime := regexp.MustCompile(`time=\S+`)
	output = reTime.ReplaceAllString(output, "time=TIME\n")

	reEventTime := regexp.MustCompile(`event_time=[^ ]+`)
	output = reEventTime.ReplaceAllString(output, "event_time=TIME\n")

	reAddr := regexp.MustCompile(`addr=127\.0\.0\.1:\d+`)
	output = reAddr.ReplaceAllString(output, "addr=127.0.0.1:port")

	reRequest := regexp.MustCompile(`request_timeout=\S+`)
	output = reRequest.ReplaceAllString(output, "request_timeout=TIMEOUTs")

	fmt.Print(output)

	// Output: time=TIME
	//  level=INFO msg="Connected to Tarantool" component=tarantool.connection event_time=TIME
	//  event=connected addr=127.0.0.1:port connection_state=connected request_timeout=TIMEOUTs

}

func TestMain(m *testing.M) {
	server := "127.0.0.1:3018"
	dialer := tarantool.NetDialer{
		Address:  server,
		User:     "test",
		Password: "test",
	}

	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       dialer,
		InitScript:   "config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("Failed to start Tarantool: %s", err)
	}

	code := m.Run()
	test_helpers.StopTarantoolWithCleanup(inst)
	os.Exit(code)
}
