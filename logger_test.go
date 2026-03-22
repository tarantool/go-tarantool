package tarantool_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/tarantool/go-tarantool/v3"
)

func ExampleNewSlogLogger() {
	var buf bytes.Buffer

	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(handler)

	opts := tarantool.Opts{
		Logger: tarantool.NewSlogLogger(logger),
	}

	ctx := context.Background()
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
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

	fmt.Print(output)

	// Output: time=TIME
	//  level=INFO msg="Connected to Tarantool" component=tarantool.connection event_time=TIME
	//  event=connected addr=127.0.0.1:port connection_state=connected

}
