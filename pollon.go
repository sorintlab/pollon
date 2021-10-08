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
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type LBType string

const (
	Random     LBType = "random"
	LeastQueue LBType = "leastqueue"
)

type ConfData struct {
	DestAddr []*net.TCPAddr
}

type Backend struct {
	destAddr   *net.TCPAddr
	closeConns chan struct{}
	needClean  bool
	connNum    int32
}

type Proxy struct {
	C                 chan ConfData
	listener          *net.TCPListener
	confMutex         sync.Mutex
	connMutex         sync.Mutex
	stop              chan struct{}
	endCh             chan error
	keepAlive         bool
	keepAliveIdle     time.Duration
	keepAliveCount    int
	keepAliveInterval time.Duration
	backends          []*Backend
	lbType            LBType
}

func NewProxy(listener *net.TCPListener) (*Proxy, error) {
	return &Proxy{
		C:        make(chan ConfData),
		listener: listener,
		stop:     make(chan struct{}),
		endCh:    make(chan error),
		lbType:   Random,
	}, nil
}

func newBackend(destAddr *net.TCPAddr) *Backend {
	return &Backend{
		destAddr:   destAddr,
		closeConns: make(chan struct{}),
	}
}

func (p *Proxy) GetBackend() *Backend {
	if p.lbType == LeastQueue {
		var backResult *Backend = nil
		var connNum int32
		for _, b := range p.backends {
			if backResult == nil {
				backResult = b
				connNum = atomic.LoadInt32(&b.connNum)
				continue
			}
			currConnNum := atomic.LoadInt32(&b.connNum)
			if connNum > currConnNum {
				backResult = b
				connNum = currConnNum
			}
		}
		return backResult
	}
	if len(p.backends) > 0 {
		return p.backends[rand.Intn(len(p.backends))]
	}
	return nil
}

func (b *Backend) incConn() {
	atomic.AddInt32(&b.connNum, 1)
}
func (b *Backend) decConn() {
	atomic.AddInt32(&b.connNum, -1)
}

// proxy client connection
func (p *Proxy) proxyConn(conn *net.TCPConn) {
	log.Printf("INFO start source connection: %v", conn)
	defer func() {
		log.Printf("closing source connection: %v", conn)
		conn.Close()
	}()
	defer conn.Close()
	p.connMutex.Lock()
	back := p.GetBackend()
	p.connMutex.Unlock()
	if back == nil {
		log.Printf("ERR no backends, closing source connection: %v", conn)
		return
	}
	p.confMutex.Lock()
	closeConns := back.closeConns
	destAddr := back.destAddr
	p.confMutex.Unlock()
	if destAddr == nil {
		log.Printf("ERR bad destAddr, closing source connection: %v", conn)
		return
	}
	back.incConn()
	defer back.decConn()

	var d net.Dialer
	d.Cancel = closeConns
	destConnInterface, err := d.Dial("tcp", destAddr.String())
	if err != nil {
		log.Printf("ERR destAddr dial, closing source connection: %v", conn)
		conn.Close()
		return
	}
	destConn := destConnInterface.(*net.TCPConn)
	defer func() {
		log.Printf("closing destination connection: %v", destConn)
		destConn.Close()
	}()

	var wg sync.WaitGroup
	end := make(chan bool)
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := io.Copy(destConn, conn)
		if err != nil {
		}
		conn.Close()
		destConn.CloseRead()
		log.Printf("ending. copied %d bytes from source to dest", n)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := io.Copy(conn, destConn)
		if err != nil {
		}
		destConn.Close()
		conn.CloseRead()
		log.Printf("ending. copied %d bytes from dest to source", n)
	}()

	go func() {
		wg.Wait()
		end <- true
	}()

	select {
	case <-end:
		log.Printf("all io copy goroutines done")
		return
	case <-closeConns:
		log.Printf("closing all connections")
		return
	}
}

//reconfig backends
func (p *Proxy) confCheck() {
	for {
		select {
		case <-p.stop:
			p.confMutex.Lock()
			// Is last iteration before func() exit, use defer
			defer p.confMutex.Unlock()
			for _, back := range p.backends {
				back.needClean = true
			}
			p.BackendCleaning()
			return
		case confData := <-p.C:
			var dAddrStr []string
			p.confMutex.Lock()
			// Add new backends
			for _, dAddr := range confData.DestAddr {
				// if New backend exists
				if !Contains(p.GetBackendsString(), dAddr.String()) {
					p.backends = append(p.backends, newBackend(dAddr))
				}
				dAddrStr = append(dAddrStr, dAddr.String())
			}
			// Delete stale backends & force close connections
			for _, back := range p.backends {
				if !Contains(dAddrStr, back.destAddr.String()) {
					back.needClean = true
				}
			}
			p.BackendCleaning()
			p.confMutex.Unlock()
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
		if p.keepAlive {
			if err := p.SetupKeepAlive(conn); err != nil {
				p.endCh <- fmt.Errorf("setKeepAlive error: %v", err)
				return
			}
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
	close(p.stop)
	if err != nil {
		return fmt.Errorf("proxy error: %v", err)
	}
	return nil
}

func (p *Proxy) GetBackendsString() []string {
	var result []string
	for _, b := range p.backends {
		result = append(result, b.destAddr.String())
	}
	return result
}

func (p *Proxy) BackendCleaning() {
	last := len(p.backends) - 1
	if last < 0 {
		return
	}
	for i := last; i >= 0; i-- {
		if p.backends[i].needClean {
			close(p.backends[i].closeConns)
			if i != last {
				p.backends[i], p.backends[last] = p.backends[last], p.backends[i]
			}
			last--
		}
	}
	if last < 0 {
		p.backends = nil
	} else {
		p.backends = p.backends[:last+1]
	}
}

func (p *Proxy) SetKeepAlive(keepalive bool) {
	p.keepAlive = keepalive
}

func (p *Proxy) SetKeepAliveIdle(d time.Duration) {
	p.keepAliveIdle = d
}

func (p *Proxy) SetKeepAliveCount(n int) {
	p.keepAliveCount = n
}

func (p *Proxy) SetKeepAliveInterval(d time.Duration) {
	p.keepAliveInterval = d
}

func (p *Proxy) SetLBType(lbt LBType) {
	p.lbType = lbt
}
