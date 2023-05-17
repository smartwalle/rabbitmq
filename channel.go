package rabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type Channel struct {
	mu                sync.Mutex
	conn              *Connection
	channel           *amqp.Channel
	closed            bool
	reconnectInterval time.Duration
	timer             *time.Timer

	confirmOpts confirmOptions
	flowOptions flowOptions

	onReconnectHandler func(*Channel)
	onCloseHandler     func(*amqp.Error)
	onFlowHandler      func(bool)
	onReturnHandler    func(amqp.Return)
	onCancelHandler    func(string)
	onPublishHandler   func(amqp.Confirmation)
}

type confirmOptions struct {
	confirming bool
	noWait     bool
}

type flowOptions struct {
	active bool
}

func newChannel(conn *Connection, reconnectInterval time.Duration) (*Channel, error) {
	var nChannel = &Channel{}
	nChannel.conn = conn
	nChannel.reconnectInterval = reconnectInterval
	if err := nChannel.connect(); err != nil {
		return nil, err
	}
	go nChannel.handleConnReconnect()
	return nChannel, nil
}

func (this *Channel) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.closed = true
	if this.timer != nil {
		this.timer.Stop()
		this.timer = nil
	}

	return this.channel.Close()
}

func (this *Channel) IsClosed() bool {
	return this.channel.IsClosed()
}

func (this *Channel) handleConnReconnect() {
	var connected = this.conn.notifyReconnect(make(chan bool, 1))
	for {
		select {
		case _, ok := <-connected:
			if !ok {
				return
			}
			this.reconnect(time.Millisecond)
		}
	}
}

func (this *Channel) connect() error {
	var channel, err = this.conn.conn.Channel()
	if err != nil {
		return err
	}
	if this.channel != nil {
		this.channel.Close()
	}
	this.channel = channel

	go this.handleNotify()

	return nil
}

func (this *Channel) handleNotify() {
	var closes = this.channel.NotifyClose(make(chan *amqp.Error, 1))
	var cancels = this.channel.NotifyCancel(make(chan string, 1))
	var flows = this.channel.NotifyFlow(make(chan bool, 1))
	var confirms = this.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	var returns = this.channel.NotifyReturn(make(chan amqp.Return, 1))

	for {
		select {
		case err := <-closes:
			if this.onCloseHandler != nil {
				this.onCloseHandler(err)
			}
			if err != nil {
				this.reconnect(this.reconnectInterval)
			}
			return
		case c := <-cancels:
			if this.onCancelHandler != nil {
				this.onCancelHandler(c)
			}
			this.reconnect(this.reconnectInterval)
			return
		case c := <-flows:
			if this.onFlowHandler != nil {
				this.onFlowHandler(c)
			}
		case c := <-confirms:
			if this.onPublishHandler != nil {
				this.onPublishHandler(c)
			}
		case r := <-returns:
			if this.onReturnHandler != nil {
				this.onReturnHandler(r)
			}
		}
	}
}

func (this *Channel) reconnect(interval time.Duration) {
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

		if this.onReconnectHandler != nil {
			this.onReconnectHandler(this)
		}

		if this.confirmOpts.confirming {
			this.channel.Confirm(this.confirmOpts.noWait)
		}

		if this.flowOptions.active {
			this.channel.Flow(this.flowOptions.active)
		}

		this.mu.Unlock()
	})
	this.mu.Unlock()
}

func (this *Channel) OnReconnect(handler func(channel *Channel)) {
	this.onReconnectHandler = handler
}

func (this *Channel) OnClose(handler func(err *amqp.Error)) {
	this.onCloseHandler = handler
}

func (this *Channel) OnCancel(handler func(c string)) {
	this.onCancelHandler = handler
}

func (this *Channel) OnFlow(handler func(c bool)) {
	this.onFlowHandler = handler
}

func (this *Channel) OnReturn(handler func(r amqp.Return)) {
	this.onReturnHandler = handler
}

func (this *Channel) OnPublish(handler func(c amqp.Confirmation)) {
	this.onPublishHandler = handler
}

func (this *Channel) Qos(prefetchCount int, prefetchSize int, global bool) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Qos(prefetchCount, prefetchSize, global)
}

func (this *Channel) Cancel(consumer string, noWait bool) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Cancel(consumer, noWait)
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
func (this *Channel) QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (this *Channel) QueueDeclarePassive(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}

func (this *Channel) QueueBind(name string, key string, exchange string, noWait bool, args amqp.Table) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.QueueBind(name, key, exchange, noWait, args)
}

func (this *Channel) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.QueueUnbind(name, key, exchange, args)
}

func (this *Channel) QueuePurge(name string, noWait bool) (int, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.QueuePurge(name, noWait)
}

func (this *Channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
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
func (this *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (this *Channel) ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (this *Channel) ExchangeDeclarePassive(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}

func (this *Channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.ExchangeDelete(name, ifUnused, noWait)
}

func (this *Channel) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.ExchangeBind(destination, key, source, noWait, args)
}

func (this *Channel) ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.ExchangeUnbind(destination, key, source, noWait, args)
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
func (this *Channel) Publish(exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.PublishWithContext(context.Background(), exchange, key, mandatory, immediate, msg)
}

func (this *Channel) PublishWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (this *Channel) PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.PublishWithDeferredConfirmWithContext(context.Background(), exchange, key, mandatory, immediate, msg)
}

func (this *Channel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (this *Channel) Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Get(queue, autoAck)
}

func (this *Channel) Tx() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Tx()
}

func (this *Channel) TxCommit() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.TxCommit()
}

func (this *Channel) TxRollback() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.TxRollback()
}

func (this *Channel) Flow(active bool) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.flowOptions = flowOptions{
		active: active,
	}

	return this.channel.Flow(active)
}

func (this *Channel) Confirm(noWait bool) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.confirmOpts = confirmOptions{
		confirming: true,
		noWait:     noWait,
	}

	return this.channel.Confirm(noWait)
}

func (this *Channel) Ack(tag uint64, multiple bool) error {
	return this.channel.Ack(tag, multiple)
}

func (this *Channel) Nack(tag uint64, multiple bool, requeue bool) error {
	return this.channel.Nack(tag, multiple, requeue)
}

func (this *Channel) Reject(tag uint64, requeue bool) error {
	return this.channel.Reject(tag, requeue)
}

func (this *Channel) GetNextPublishSeqNo() uint64 {
	return this.channel.GetNextPublishSeqNo()
}
