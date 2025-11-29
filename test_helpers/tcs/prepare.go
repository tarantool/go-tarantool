package tcs

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
	"time"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

const (
	waitTimeout  = 500 * time.Millisecond
	connectRetry = 3
	tcsUser      = "client"
	tcsPassword  = "secret"
)

//go:embed testdata/config.yaml
var tcsConfig []byte

func writeConfig(name string, port int) error {
	cfg, err := os.Create(name)
	if err != nil {
		return err
	}
	defer cfg.Close()

	err = cfg.Chmod(0644)
	if err != nil {
		return err
	}

	t := template.Must(template.New("config").Parse(string(tcsConfig)))
	return t.Execute(cfg, map[string]interface{}{
		"host": "localhost",
		"port": port,
	})
}

func makeOpts(port int) (test_helpers.StartOpts, error) {
	opts := test_helpers.StartOpts{}
	var err error
	opts.WorkDir, err = os.MkdirTemp("", "tcs_dir")
	if err != nil {
		return opts, err
	}

	opts.ConfigFile = filepath.Join(opts.WorkDir, "config.yaml")
	err = writeConfig(opts.ConfigFile, port)
	if err != nil {
		return opts, fmt.Errorf("can't save file %q: %w", opts.ConfigFile, err)
	}

	opts.Listen = fmt.Sprintf("localhost:%d", port)
	opts.WaitStart = waitTimeout
	opts.ConnectRetry = connectRetry
	opts.RetryTimeout = waitTimeout
	opts.InstanceName = "master"
	opts.Dialer = tarantool.NetDialer{
		Address:  opts.Listen,
		User:     tcsUser,
		Password: tcsPassword,
	}
	return opts, nil
}
