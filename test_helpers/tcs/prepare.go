package tcs

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

const (
	waitTimeout  = 500 * time.Millisecond
	connectRetry = 3
	TcsUser      = "client"
	TcsPassword  = "secret"
)

//go:embed testdata/config.yaml
var tcsConfig []byte

func makeOpts(port int) (test_helpers.StartOpts, error) {
	opts := test_helpers.StartOpts{}
	dir, err := os.MkdirTemp("", "tcs_dir")
	if err != nil {
		return opts, err
	}
	os.WriteFile(filepath.Join(dir, "config.yaml"), tcsConfig, 0644)

	address := fmt.Sprintf("localhost:%d", port)

	opts = test_helpers.StartOpts{
		ConfigFile:   "config.yaml",
		WorkDir:      dir,
		WaitStart:    waitTimeout,
		ConnectRetry: connectRetry,
		RetryTimeout: waitTimeout,
		InstanceName: "master",
		Listen:       address,
		Dialer: tarantool.NetDialer{
			Address:  address,
			User:     TcsUser,
			Password: TcsPassword,
		},
	}
	return opts, nil
}
