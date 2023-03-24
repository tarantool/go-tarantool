package tarantool

import (
	"net"
	"time"
)

type deadlineIO struct {
	to time.Duration
	c  net.Conn
}

func (d *deadlineIO) Write(b []byte) (n int, err error) {
	if d.to > 0 {
		d.c.SetWriteDeadline(time.Now().Add(d.to))
	}
	n, err = d.c.Write(b)
	return
}

func (d *deadlineIO) Read(b []byte) (n int, err error) {
	if d.to > 0 {
		d.c.SetReadDeadline(time.Now().Add(d.to))
	}
	n, err = d.c.Read(b)
	return
}
