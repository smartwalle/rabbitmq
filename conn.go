package rabbitmq

import (
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Conn struct {
	conn                *amqp.Connection
	closed              atomic.Bool
	stateChangedHandler atomic.Value
}

func New(url string, config Config) (*Conn, error) {
	var nConn = &Conn{}

	conn, err := amqp.DialConfig(url, config)
	if err != nil {
		return nil, err
	}
	nConn.conn = conn
	nConn.watchState(conn)
	return nConn, nil
}

func (c *Conn) OnStateChanged(handler StateChangedHandler) {
	if handler == nil {
		return
	}
	c.stateChangedHandler.Store(handler)
}

func (c *Conn) channel(closable bool) (*channel, error) {
	conn, err := c.currentConn()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &channel{Channel: ch, closable: closable}, nil
}

func (c *Conn) Channel() (Channel, error) {
	return c.channel(true)
}

func (c *Conn) Producer(confirm bool) (Producer, error) {
	ch, err := c.channel(false)
	if err != nil {
		return nil, err
	}
	if confirm {
		if err = ch.Confirm(false); err != nil {
			_ = ch.close()
			return nil, err
		}
	}

	return &producer{
		ch:      ch,
		confirm: confirm,
	}, nil
}

func (c *Conn) Consumer(queue, consumerTag string, autoAck, exclusive, noLocal, noWait bool, args Table) (Consumer, error) {
	if queue == "" {
		return nil, ErrAnonymousQueue
	}

	ch, err := c.channel(false)
	if err != nil {
		return nil, err
	}

	return &consumer{
		ch:          ch,
		queue:       queue,
		consumerTag: consumerTag,
		autoAck:     autoAck,
		exclusive:   exclusive,
		noLocal:     noLocal,
		noWait:      noWait,
		args:        args,
	}, nil
}

func (c *Conn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Conn) currentConn() (*amqp.Connection, error) {
	if c.closed.Load() {
		return nil, ErrConnClosed
	}
	if c.conn == nil || c.conn.IsClosed() {
		return nil, ErrNotConnected
	}
	return c.conn, nil
}

func (c *Conn) Closed() bool {
	return c.closed.Load()
}

func (c *Conn) watchState(conn *amqp.Connection) {
	var stateChan = make(chan *StateChanged, 16)
	conn.NotifyStateChange(stateChan)

	go func() {
		for state := range stateChan {
			c.handleStateChange(state)
		}
	}()
}

func (c *Conn) handleStateChange(state *StateChanged) {
	var handler, _ = c.stateChangedHandler.Load().(StateChangedHandler)
	if handler == nil {
		return
	}

	defer func() {
		_ = recover()
	}()

	handler(state)
}
