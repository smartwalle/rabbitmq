package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type Conn struct {
	mu         sync.Mutex
	conn       *amqp.Connection
	url        string
	config     Config
	closed     bool
	reconnects []chan bool
	timer      *time.Timer
}

func NewConn(url string, config Config) (*Conn, error) {
	if config.ReconnectInterval <= 0 {
		config.ReconnectInterval = time.Second * 5
	}

	var nConn = &Conn{}
	nConn.url = url
	nConn.config = config
	if err := nConn.connect(); err != nil {
		return nil, err
	}
	return nConn, nil
}

func (this *Conn) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.closed = true
	if this.timer != nil {
		this.timer.Stop()
		this.timer = nil
	}
	for _, c := range this.reconnects {
		close(c)
	}

	return this.conn.Close()
}

func (this *Conn) IsClosed() bool {
	return this.conn.IsClosed()
}

func (this *Conn) handleNotify() {
	var closed = this.conn.NotifyClose(make(chan *amqp.Error, 1))
	select {
	case err := <-closed:
		if err != nil {
			this.reconnect(this.config.ReconnectInterval)
		}
	}
}

func (this *Conn) connect() error {
	var conn, err = amqp.DialConfig(this.url, this.config.Config)
	if err != nil {
		return err
	}
	if this.conn != nil {
		this.conn.Close()
	}
	this.conn = conn

	go this.handleNotify()

	return nil
}

func (this *Conn) reconnect(interval time.Duration) {
	this.mu.Lock()
	if this.closed {
		this.mu.Unlock()
		return
	}

	if this.timer != nil {
		this.timer.Stop()
	}

	this.timer = time.AfterFunc(interval, func() {
		this.mu.Lock()
		if this.closed {
			this.mu.Unlock()
			return
		}

		var err = this.connect()
		if err != nil {
			this.mu.Unlock()
			this.reconnect(interval)
			return
		}

		this.timer.Stop()
		this.timer = nil

		for _, c := range this.reconnects {
			c <- true
		}
		this.mu.Unlock()
	})
	this.mu.Unlock()
}

func (this *Conn) NotifyReconnect(c chan bool) chan bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		close(c)
	} else {
		this.reconnects = append(this.reconnects, c)
	}
	return c
}

func (this *Conn) Channel() (*Channel, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	return newChannel(this, this.config)
}
