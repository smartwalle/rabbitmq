package rabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

const (
	optChannelFlow    = 1
	optChannelConfirm = 2
)

type Channel struct {
	mu                sync.Mutex
	conn              *Connection
	channel           *amqp.Channel
	reconnectInterval time.Duration

	close     chan struct{}
	closeOnce sync.Once

	reconnectOptions map[int]channelReconnectOption

	reconnectHandle func(*Channel)
	closeHandler    func(*Error)
	cancelHandler   func(string)

	flowHandler    func(bool)
	returnHandler  func(Return)
	publishHandler func(Confirmation)

	overflowed bool
}

type channelReconnectOption func(channel *amqp.Channel)

func withConfirm(noWait bool) channelReconnectOption {
	return func(channel *amqp.Channel) {
		channel.Confirm(noWait)
	}
}

func withFlow(active bool) channelReconnectOption {
	return func(channel *amqp.Channel) {
		channel.Flow(active)
	}
}

func withQos(prefetchCount int, prefetchSize int, global bool) channelReconnectOption {
	return func(channel *amqp.Channel) {
		channel.Qos(prefetchCount, prefetchSize, global)
	}
}

func newChannel(conn *Connection, reconnectInterval time.Duration) (*Channel, error) {
	var nChannel = &Channel{}
	nChannel.conn = conn
	nChannel.close = make(chan struct{})
	nChannel.reconnectInterval = reconnectInterval
	if err := nChannel.connect(); err != nil {
		return nil, err
	}
	return nChannel, nil
}

func (c *Channel) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closeOnce.Do(func() {
		close(c.close)
	})
	c.reconnectOptions = nil

	return c.channel.Close()
}

func (c *Channel) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.IsClosed()
}

func (c *Channel) Overflowed() bool {
	return c.overflowed
}

func (c *Channel) connect() error {
	var channel, err = c.conn.conn.Channel()
	if err != nil {
		return err
	}
	if c.channel != nil {
		c.channel.Close()
	}
	c.channel = channel
	c.overflowed = false

	go c.handleNotify()

	return nil
}

func (c *Channel) handleNotify() {
	var closes = c.channel.NotifyClose(make(chan *Error, 1))
	var cancels = c.channel.NotifyCancel(make(chan string, 1))
	var flows = c.channel.NotifyFlow(make(chan bool, 1))
	var confirms = c.channel.NotifyPublish(make(chan Confirmation, 1))
	var returns = c.channel.NotifyReturn(make(chan Return, 1))

	for {
		select {
		case err := <-closes:
			if c.closeHandler != nil {
				c.closeHandler(err)
			}
			if err != nil {
				c.reconnect(c.reconnectInterval)
			}
			return
		case value := <-cancels:
			if c.cancelHandler != nil {
				c.cancelHandler(value)
			}
			c.reconnect(c.reconnectInterval)
			return
		case value := <-flows:
			c.overflowed = value
			if c.flowHandler != nil {
				c.flowHandler(value)
			}
		case value := <-confirms:
			if c.publishHandler != nil {
				c.publishHandler(value)
			}
		case value := <-returns:
			if c.returnHandler != nil {
				c.returnHandler(value)
			}
		}
	}
}

func (c *Channel) reconnect(interval time.Duration) {
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
				opt(c.channel)
			}
		}
		c.mu.Unlock()

		if c.reconnectHandle != nil {
			c.reconnectHandle(c)
		}
		return
	}
}

func (c *Channel) addReconnectOptions(key int, fn channelReconnectOption) {
	if fn == nil {
		return
	}
	select {
	case <-c.close:
		return
	default:
	}

	if c.reconnectOptions == nil {
		c.reconnectOptions = make(map[int]channelReconnectOption)
	}
	c.reconnectOptions[key] = fn
}

func (c *Channel) OnReconnect(handler func(channel *Channel)) {
	c.reconnectHandle = handler
}

func (c *Channel) OnClose(handler func(err *Error)) {
	c.closeHandler = handler
}

func (c *Channel) OnCancel(handler func(c string)) {
	c.cancelHandler = handler
}

func (c *Channel) OnFlow(handler func(c bool)) {
	c.flowHandler = handler
}

func (c *Channel) OnReturn(handler func(r Return)) {
	c.returnHandler = handler
}

func (c *Channel) OnPublish(handler func(c Confirmation)) {
	c.publishHandler = handler
}

func (c *Channel) Qos(prefetchCount int, prefetchSize int, global bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.addReconnectOptions(3, withQos(prefetchCount, prefetchSize, global))

	return c.channel.Qos(prefetchCount, prefetchSize, global)
}

func (c *Channel) Cancel(consumer string, noWait bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.Cancel(consumer, noWait)
}

// QueueDeclare
//
// name - 队列名称
//
// durable - 是否持久化
//
// autoDelete - 是否自动删除
//
// exclusive - 是否独占
//
// noWait - 是否阻塞
//
// args - 其它参数
func (c *Channel) QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args Table) (Queue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (c *Channel) QueueDeclarePassive(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args Table) (Queue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}

func (c *Channel) QueueBind(name string, key string, exchange string, noWait bool, args Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.QueueBind(name, key, exchange, noWait, args)
}

func (c *Channel) QueueUnbind(name, key, exchange string, args Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.QueueUnbind(name, key, exchange, args)
}

func (c *Channel) QueuePurge(name string, noWait bool) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.QueuePurge(name, noWait)
}

func (c *Channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

// Consume
//
// queue - 队列名称
//
// consumer - 消费者名称
//
// autoAck - 是否自动应答
//
// exclusive - 是否独占
//
// noLocal - 设置为 true，表示不能将同一个 Connection 中生产者发送的消息传递给这个 Connection 中的消费者
//
// noWait - 是否阻塞
//
// args - 其它参数
func (c *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args Table) (<-chan Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *Channel) ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c *Channel) ExchangeDeclarePassive(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c *Channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.ExchangeDelete(name, ifUnused, noWait)
}

func (c *Channel) ExchangeBind(destination, key, source string, noWait bool, args Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.ExchangeBind(destination, key, source, noWait, args)
}

func (c *Channel) ExchangeUnbind(destination, key, source string, noWait bool, args Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.ExchangeUnbind(destination, key, source, noWait, args)
}

// Publish
//
// exchange - 交换机名称
//
// key - Key
//
// mandatory - 如果为 true，根据自身 exchange 类型和 route key 规则无法找到符合条件的队列会把消息返还给发送者
//
// immediate - 如果为 true，当 exchange 发送消息到队列后发现队列上没有消费者，则会把消息返还给发送者，在 RabbitMQ 3.0以后的版本里，去掉了immediate参数的支持
//
// msg - 消息内容
func (c *Channel) Publish(exchange string, key string, mandatory bool, immediate bool, msg Publishing) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.PublishWithContext(context.Background(), exchange, key, mandatory, immediate, msg)
}

func (c *Channel) PublishWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg Publishing) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (c *Channel) PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg Publishing) (*DeferredConfirmation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.PublishWithDeferredConfirmWithContext(context.Background(), exchange, key, mandatory, immediate, msg)
}

func (c *Channel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg Publishing) (*DeferredConfirmation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (c *Channel) Get(queue string, autoAck bool) (msg Delivery, ok bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.Get(queue, autoAck)
}

func (c *Channel) Flow(active bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.addReconnectOptions(optChannelFlow, withFlow(active))

	return c.channel.Flow(active)
}

func (c *Channel) Confirm(noWait bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.addReconnectOptions(optChannelConfirm, withConfirm(noWait))

	return c.channel.Confirm(noWait)
}

func (c *Channel) Ack(tag uint64, multiple bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.Ack(tag, multiple)
}

func (c *Channel) Nack(tag uint64, multiple bool, requeue bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.Nack(tag, multiple, requeue)
}

func (c *Channel) Reject(tag uint64, requeue bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.Reject(tag, requeue)
}

func (c *Channel) GetNextPublishSeqNo() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel.GetNextPublishSeqNo()
}
