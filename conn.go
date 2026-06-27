package rabbitmq

import (
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

	reconnectHandler atomic.Value
	closeHandler     atomic.Value
	blockHandler     atomic.Value

	blocked uint32
}

type reconnectOption func(conn *amqp.Connection)

func withSecret(secret, reason string) reconnectOption {
	return func(conn *amqp.Connection) {
		_ = conn.UpdateSecret(secret, reason)
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
	return atomic.LoadUint32(&c.blocked) == 1
}

func (c *Connection) handleNotify(conn *amqp.Connection) {
	var closes = conn.NotifyClose(make(chan *Error, 1))
	var blocks = conn.NotifyBlocked(make(chan Blocking, 1))

	for {
		select {
		case err, ok := <-closes:
			if handler := c.closeHandler.Load(); handler != nil {
				handler.(func(*Error))(err)
			}
			if ok && err != nil {
				c.reconnect(c.config.ReconnectInterval)
			}
			return
		case block, ok := <-blocks:
			if !ok {
				blocks = nil
				continue
			}
			if block.Active {
				atomic.StoreUint32(&c.blocked, 1)
			} else {
				atomic.StoreUint32(&c.blocked, 0)
			}
			if handler := c.blockHandler.Load(); handler != nil {
				handler.(func(Blocking))(block)
			}
		}
	}
}

func (c *Connection) connect() error {
	var conn, err = amqp.DialConfig(c.url, c.config.Config)
	if err != nil {
		return err
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = conn
	atomic.StoreUint32(&c.blocked, 0)

	go c.handleNotify(conn)

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

		if handler := c.reconnectHandler.Load(); handler != nil {
			handler.(func(*Connection))(c)
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
	if handler == nil {
		return
	}
	c.reconnectHandler.Store(handler)
}

func (c *Connection) OnClose(handler func(err *Error)) {
	if handler == nil {
		return
	}
	c.closeHandler.Store(handler)
}

func (c *Connection) Channel() (*Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return newChannel(c, c.config.ReconnectInterval)
}
