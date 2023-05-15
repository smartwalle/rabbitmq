package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type Conn struct {
	mu     sync.Mutex
	conn   *amqp.Connection
	url    string
	config Config

	notifyMu   sync.Mutex
	noNotify   bool
	reconnects []chan bool
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
	this.notifyMu.Lock()
	this.noNotify = true
	for _, c := range this.reconnects {
		close(c)
	}
	this.notifyMu.Unlock()

	this.mu.Lock()
	defer this.mu.Unlock()
	return this.conn.Close()
}

func (this *Conn) IsClosed() bool {
	return this.conn.IsClosed()
}

func (this *Conn) connect() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	conn, err := amqp.DialConfig(this.url, this.config.Config)
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

func (this *Conn) handleNotify() {
	var closed = this.conn.NotifyClose(make(chan *amqp.Error, 1))
	select {
	case err := <-closed:
		if err != nil {
			this.reconnect()
		}
	}
}

func (this *Conn) reconnect() {
	for {
		time.Sleep(this.config.ReconnectInterval)
		var err = this.connect()
		if err == nil {
			this.notifyMu.Lock()
			for _, c := range this.reconnects {
				c <- true
			}
			this.notifyMu.Unlock()
			return
		}
	}
}

func (this *Conn) NotifyReconnect(c chan bool) chan bool {
	this.notifyMu.Lock()
	defer this.notifyMu.Unlock()

	if this.noNotify {
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
