package pool4go

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

var ErrClosedPool = errors.New("pool4go: get on closed pool")

type Pool interface {
	Get() (c Conn, err error)

	Put(c Conn)

	Release(c Conn)

	Close() error

	MaxIdle() int

	MaxOpen() int
}

type Option func(p *pool)

func WithMaxIdle(n int) Option {
	return func(p *pool) {
		p.maxIdleConn = n
	}
}

func WithMaxOpen(n int) Option {
	return func(p *pool) {
		p.maxOpenConn = n
	}
}

func WithTestOnBorrow(f func(conn Conn, t time.Time) error) Option {
	return func(p *pool) {
		p.testOnBorrowFunc = f
	}
}

func WithIdleTimeout(t time.Duration) Option {
	return func(p *pool) {
		p.idleTimeout = t
	}
}

type pool struct {
	newFunc          func() (Conn, error)
	testOnBorrowFunc func(conn Conn, t time.Time) error

	numOpenConn int
	maxIdleConn int
	maxOpenConn int
	idleTimeout time.Duration

	running  bool
	idleList *list.List
	mu       *sync.Mutex
	cond     *sync.Cond
}

const (
	kMaxIdle = 2
	KMaxOpen = 4
)

func New(dialFunc func() (Conn, error), opts ...Option) Pool {
	var p = &pool{}
	p.newFunc = dialFunc
	p.maxIdleConn = kMaxIdle
	p.maxOpenConn = KMaxOpen
	p.numOpenConn = 0
	p.running = true
	p.idleList = list.New()
	p.mu = &sync.Mutex{}
	p.cond = sync.NewCond(p.mu)

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (this *pool) Get() (c Conn, err error) {
	c, err = this.get()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (this *pool) get() (c Conn, err error) {
	for {
		this.mu.Lock()
		var item = this.idleList.Front()
		if item != nil && item.Value != nil {
			if idleConn, ok := item.Value.(*idleConn); ok {
				this.idleList.Remove(item)
				if this.idleTimeout > 0 && time.Now().After(idleConn.t.Add(this.idleTimeout)) {
					this.release(idleConn.c)
					this.mu.Unlock()
					continue
				}
				if this.testOnBorrowFunc != nil {
					if err = this.testOnBorrowFunc(idleConn.c, idleConn.t); err != nil {
						this.release(idleConn.c)
						this.mu.Unlock()
						continue
					}
				}
				this.mu.Unlock()
				return idleConn.c, nil
			}
		}

		if this.running == false {
			this.mu.Unlock()
			return nil, ErrClosedPool
		}

		if this.maxOpenConn <= 0 || this.numOpenConn < this.maxOpenConn {
			c, err := this.newFunc()
			if err != nil {
				c = nil
			}
			if c != nil {
				this.numOpenConn += 1
			}
			this.mu.Unlock()
			return c, err
		}

		this.cond.Wait()
		this.mu.Unlock()
	}
	return nil, nil
}

func (this *pool) Put(c Conn) {
	if c != nil {
		this.put(c, false)
	}
}

func (this *pool) put(c Conn, close bool) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if c == nil {
		return
	}

	if this.running == false {
		this.release(c)
		return
	}

	if close == false {
		this.idleList.PushFront(&idleConn{t: time.Now(), c: c})
		if this.idleList.Len() > this.maxIdleConn {
			c = this.idleList.Remove(this.idleList.Back()).(*idleConn).c
		} else {
			c = nil
		}
	}

	this.release(c)
}

func (this *pool) Release(c Conn) {
	if c != nil {
		this.put(c, true)
	}
}

func (this *pool) release(c Conn) {
	if c != nil {
		c.Close()
		this.numOpenConn -= 1
	}
	this.cond.Signal()
}

func (this *pool) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.running == false {
		return nil
	}
	this.running = false
	this.numOpenConn = 0
	this.cond.Broadcast()
	for item := this.idleList.Front(); item != nil; item = item.Next() {
		item.Value.(*idleConn).c.Close()
	}
	this.idleList.Init()
	return nil
}

func (this *pool) MaxIdle() int {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.maxIdleConn
}

func (this *pool) MaxOpen() int {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.maxOpenConn
}
