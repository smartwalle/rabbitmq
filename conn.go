package rabbitmq

import (
	"crypto/tls"
	amqp "github.com/rabbitmq/amqp091-go"
	"net"
	"sync"
	"time"
)

const (
	optConnUpdateSecret = 1
)

type Connection struct {
	mu     sync.Mutex
	conn   *amqp.Connection
	url    string
	config Config

	close     chan struct{}
	closeOnce sync.Once

	reconnectOptions map[int]reconnectOption

	reconnectHandler func(*Connection)
	closeHandler     func(*Error)
	blockHandler     func(Blocking)

	blocked bool
}

type reconnectOption func(conn *amqp.Connection)

func withSecret(secret, reason string) reconnectOption {
	return func(conn *amqp.Connection) {
		conn.UpdateSecret(secret, reason)
	}
}

func NewConn(url string, config Config) (*Connection, error) {
	if config.ReconnectInterval <= 0 {
		config.ReconnectInterval = time.Second * 5
	}

	var nConn = &Connection{}
	nConn.url = url
	nConn.config = config
	nConn.close = make(chan struct{})
	if err := nConn.connect(); err != nil {
		return nil, err
	}
	return nConn, nil
}

func (c *Connection) UpdateSecret(newSecret, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.addReconnectOptions(optConnUpdateSecret, withSecret(newSecret, reason))

	return c.conn.UpdateSecret(newSecret, reason)
}

func (c *Connection) LocalAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.LocalAddr()
}

func (c *Connection) RemoteAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.RemoteAddr()
}

func (c *Connection) ConnectionState() tls.ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.ConnectionState()
}

func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closeOnce.Do(func() {
		close(c.close)
	})
	c.reconnectOptions = nil

	return c.conn.Close()
}

func (c *Connection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.IsClosed()
}

func (c *Connection) Blocked() bool {
	return c.blocked
}

func (c *Connection) handleNotify() {
	var closes = c.conn.NotifyClose(make(chan *Error, 1))
	var blocks = c.conn.NotifyBlocked(make(chan Blocking, 1))

	select {
	case err := <-closes:
		if c.closeHandler != nil {
			c.closeHandler(err)
		}
		if err != nil {
			c.reconnect(c.config.ReconnectInterval)
		}
	case block := <-blocks:
		c.blocked = block.Active
		if c.blockHandler != nil {
			c.blockHandler(block)
		}
	}
}

func (c *Connection) connect() error {
	var conn, err = amqp.DialConfig(c.url, c.config.Config)
	if err != nil {
		return err
	}
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = conn

	go c.handleNotify()

	return nil
}

func (c *Connection) reconnect(interval time.Duration) {
	c.mu.Lock()

	for {
		select {
		case <-time.After(interval):
		case <-c.close:
			c.mu.Unlock()
			return
		}

		var err = c.connect()
		if err != nil {
			continue
		}

		for _, opt := range c.reconnectOptions {
			if opt != nil {
				opt(c.conn)
			}
		}
		c.mu.Unlock()

		if c.reconnectHandler != nil {
			c.reconnectHandler(c)
		}
		return
	}
}

func (c *Connection) addReconnectOptions(key int, fn reconnectOption) {
	if fn == nil {
		return
	}
	select {
	case <-c.close:
		return
	default:
	}

	if c.reconnectOptions == nil {
		c.reconnectOptions = make(map[int]reconnectOption)
	}
	c.reconnectOptions[key] = fn
}

func (c *Connection) OnReconnect(handler func(conn *Connection)) {
	c.reconnectHandler = handler
}

func (c *Connection) OnClose(handler func(err *Error)) {
	c.closeHandler = handler
}

func (c *Connection) Channel() (*Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return newChannel(c, c.config.ReconnectInterval)
}
