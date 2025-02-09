package pool

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/tarantool/go-tarantool/v2/box"
)

func (p *ConnectionPool) StartDiscovery() error {
	if p.discoveringCancel != nil {
		return errors.New("discovering already started")
	}

	ctx, cancel := context.WithCancel(p.ctx)

	t := time.NewTicker(p.opts.DiscoveryTimeout)

	go func() {
		for {
			select {
			case <-t.C:
				// we use any connection, because master can be unavailable
				info, err := box.New(NewConnectorAdapter(p, ANY)).Info()
				if err != nil {
					log.Printf("tarantool: watch topology failed: %s\n", err)
					continue
				}

				for _, replication := range info.Replication {
					upstream := replication.Upstream

					if upstream.Status != "follow" {
						log.Printf("found replication instance (%s:%s) in non-follow state; skip discovering\n",
							upstream.Name, upstream.Peer)
						continue
					}

					addr := upstream.Peer

					if addr == "" { // itself instance
						continue
					}

					name := upstream.Name

					if name == "" {
						name = replication.UUID
					}

					p.endsMutex.Lock()
					_, exists := p.ends[addr]
					p.endsMutex.Unlock()

					if !exists {
						dialer := p.opts.DiscoveringDialer
						dialer.Address = addr

						i := Instance{
							Name:   name,
							Dialer: dialer,
						}

						err = p.Add(ctx, i)
						if err != nil {
							log.Printf("tarantool: add to pool failed: %s\n", err)
							continue
						}
					}
				}

				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	p.discoveringCancel = cancel

	return nil
}

func (p *ConnectionPool) StopDiscovery() error {
	if p.discoveringCancel != nil {
		p.discoveringCancel()
		p.discoveringCancel = nil

		return nil
	}

	return errors.New("discovering not started yet")
}
