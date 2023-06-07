package rabbitmq

import (
	"crypto/tls"
	amqp "github.com/rabbitmq/amqp091-go"
	"net"
	"sync"
	"time"
)

type Connection struct {
	mu         sync.Mutex
	conn       *amqp.Connection
	url        string
	config     Config
	closed     bool
	reconnects []chan bool
	timer      *time.Timer

	secretOpts secretOptions

	onReconnect func(*Connection)
	onClose     func(*amqp.Error)
}

type secretOptions struct {
	secret string
	reason string
}

func NewConn(url string, config Config) (*Connection, error) {
	if config.ReconnectInterval <= 0 {
		config.ReconnectInterval = time.Second * 5
	}

	var nConn = &Connection{}
	nConn.url = url
	nConn.config = config
	if err := nConn.connect(); err != nil {
		return nil, err
	}
	return nConn, nil
}

func (this *Connection) UpdateSecret(newSecret, reason string) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.secretOpts = secretOptions{
		secret: newSecret,
		reason: reason,
	}

	return this.conn.UpdateSecret(newSecret, reason)
}

func (this *Connection) LocalAddr() net.Addr {
	return this.conn.LocalAddr()
}

func (this *Connection) RemoteAddr() net.Addr {
	return this.conn.RemoteAddr()
}

func (this *Connection) ConnectionState() tls.ConnectionState {
	return this.conn.ConnectionState()
}

func (this *Connection) Close() error {
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
	this.reconnects = nil

	return this.conn.Close()
}

func (this *Connection) IsClosed() bool {
	return this.conn.IsClosed()
}

func (this *Connection) handleNotify() {
	var closed = this.conn.NotifyClose(make(chan *amqp.Error, 1))
	select {
	case err := <-closed:
		if this.onClose != nil {
			this.onClose(err)
		}
		if err != nil {
			this.reconnect(this.config.ReconnectInterval)
		}
	}
}

func (this *Connection) connect() error {
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

func (this *Connection) reconnect(interval time.Duration) {
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

		if this.onReconnect != nil {
			this.onReconnect(this)
		}

		if this.secretOpts.secret != "" {
			this.conn.UpdateSecret(this.secretOpts.secret, this.secretOpts.reason)
		}

		this.mu.Unlock()
	})
	this.mu.Unlock()
}

func (this *Connection) notifyReconnect(c chan bool) chan bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		close(c)
	} else {
		this.reconnects = append(this.reconnects, c)
	}
	return c
}

func (this *Connection) OnReconnect(handler func(conn *Connection)) {
	this.onReconnect = handler
}

func (this *Connection) OnClose(handler func(err *amqp.Error)) {
	this.onClose = handler
}

func (this *Connection) Channel() (*Channel, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	return newChannel(this, this.config.ReconnectInterval)
}
