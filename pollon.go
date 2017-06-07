// Copyright 2015 Sorint.lab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package pollon

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type ConfData struct {
	DestAddr *net.TCPAddr
}

type Proxy struct {
	listener *net.TCPListener

	configMutex sync.Mutex
	C           chan ConfData
	destAddr    *net.TCPAddr

	connMutex  sync.Mutex
	closeConns chan struct{}
	stopCh     chan struct{}
	endCh      chan error

	limiter *time.Ticker
	state   int
}

const (
	proxyActive = iota
	proxyPaused
)

func NewProxy(listener *net.TCPListener) (*Proxy, error) {
	return &Proxy{
		listener:    listener,
		configMutex: sync.Mutex{},
		C:           make(chan ConfData),
		connMutex:   sync.Mutex{},
		closeConns:  make(chan struct{}),
		stopCh:      make(chan struct{}),
		endCh:       make(chan error),
	}, nil
}

func (p *Proxy) proxyConn(conn *net.TCPConn) {
	p.connMutex.Lock()
	if p.limiter != nil {
		<-p.limiter.C
	}
	closeConns := p.closeConns
	destAddr := p.destAddr
	p.connMutex.Unlock()

	defer func() {
		log.Printf("closing source connection: %v", conn)
		conn.Close()
	}()

	if destAddr == nil {
		log.Print("can't open connection: destination address empty")
		return
	}

	destConn, err := net.DialTCP("tcp", nil, p.destAddr)
	if err != nil {
		log.Printf("failed to open connection to destination: %v", err)
		return
	}
	defer func() {
		log.Printf("closing destination connection: %v", destConn)
		destConn.Close()
	}()

	var wg sync.WaitGroup
	end := make(chan struct{})
	wg.Add(1)
	go func() {
		n, err := io.Copy(destConn, conn)
		if err != nil {
			log.Printf("error copying from source to dest: %v", err)
		}
		conn.CloseWrite()
		destConn.CloseRead()
		log.Printf("ending. copied %d bytes from source to dest", n)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		n, err := io.Copy(conn, destConn)
		if err != nil {
			log.Printf("error copying from dest to source: %v", err)
		}
		destConn.CloseWrite()
		conn.CloseRead()
		log.Printf("ending. copied %d bytes from dest to source", n)
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(end)
	}()

	select {
	case <-end:
		log.Printf("connection closing - all copy goroutines done")
		return
	case <-closeConns:
		log.Printf("closing all connections")
		return
	}
}

func (p *Proxy) confCheck() {
	for {
		select {
		case <-p.stopCh:
			return
		case confData := <-p.C:
			if confData.DestAddr.String() != p.destAddr.String() {
				p.connMutex.Lock()
				if p.closeConns != nil {
					close(p.closeConns)
				}
				p.closeConns = make(chan struct{})
				p.destAddr = confData.DestAddr
				p.connMutex.Unlock()
			}
		}
	}
}

func (p *Proxy) accepter() {
	for {
		conn, err := p.listener.AcceptTCP()
		if err != nil {
			p.endCh <- fmt.Errorf("accept error: %v", err)
			return
		}
		go p.proxyConn(conn)
	}
}

func (p *Proxy) Stop() {
	p.endCh <- nil
}

func (p *Proxy) Start() error {
	go p.confCheck()
	go p.accepter()
	err := <-p.endCh
	close(p.stopCh)
	if err != nil {
		return fmt.Errorf("proxy error: %v", err)
	}
	return nil
}

// Pause makes proxy continue accepting new connections, but prevents it
// from opening new connections to destination address until proxy status
// changes.
// It doesn't affect already open connections in any way.
func (p *Proxy) Pause() {
	p.configMutex.Lock()
	if p.state == proxyActive {
		p.state = proxyPaused
		p.connMutex.Unlock()
	}
	p.configMutex.Unlock()
}

// Resume makes a paused proxy resume opening news connections to destination
// address.
// It makes no attempt to spread backlogged connection opens over time, so it
// can cause thundering herd problem - use with caution
func (p *Proxy) Resume() {
	p.configMutex.Lock()
	if p.state == proxyPaused {
		p.state = proxyActive
		if p.closeConns == nil {
			p.closeConns = make(chan struct{})
		}
		p.connMutex.Unlock()
	}
	p.configMutex.Unlock()
}

// PauseAndDisconnect first pauses the proxy and then, after timeout t has
// passed, closes all connections opened prior to the proxy being paused.
// It can be used for a graceful switchover when paired with client-side
// connection timeout
func (p *Proxy) PauseAndDisconnect(t time.Duration) {
	p.configMutex.Lock()

	if p.state == proxyActive {
		p.state = proxyPaused
		p.connMutex.Lock()

		closeConns := p.closeConns
		p.closeConns = nil
		go func() {
			<-time.NewTimer(t).C
			close(closeConns)
		}()
	}

	p.configMutex.Unlock()
}

// GradualResume makes the proxy resume opening connections to destination,
// but to avoid thundering herd problem by opening  at the rate of one
// connection every interval i for the period of duration d
func (p *Proxy) GradualResume(i, d time.Duration) {
	p.configMutex.Lock()
	if p.state == proxyPaused {
		p.state = proxyActive
		if p.closeConns == nil {
			p.closeConns = make(chan struct{})
		}
		p.limit(i, d)
		p.connMutex.Unlock()
	}
	p.configMutex.Unlock()
}

func (p *Proxy) limit(i, d time.Duration) {
	if i.Nanoseconds() <= 0 || d.Nanoseconds() <= 0 {
		return
	}
	p.limiter = time.NewTicker(i)
	go func() {
		<-time.NewTimer(d).C
		p.connMutex.Lock()
		p.limiter = nil
		p.connMutex.Unlock()
	}()
}

// Config atomically updates proxy destination address without affecting
// already open connections
func (p *Proxy) Config(conf ConfData) {
	p.configMutex.Lock()
	if conf.DestAddr.String() != p.destAddr.String() {
		p.connMutex.Lock()
		p.destAddr = conf.DestAddr
		p.connMutex.Unlock()
	}
	p.configMutex.Unlock()
}
