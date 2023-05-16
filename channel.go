package rabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type Channel struct {
	mu         sync.Mutex
	conn       *Conn
	channel    *amqp.Channel
	config     Config
	closed     bool
	reconnects []chan bool
	timer      *time.Timer
}

func newChannel(conn *Conn, config Config) (*Channel, error) {
	var nChannel = &Channel{}
	nChannel.conn = conn
	nChannel.config = config
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
	for _, c := range this.reconnects {
		close(c)
	}

	return this.channel.Close()
}

func (this *Channel) IsClosed() bool {
	return this.channel.IsClosed()
}

func (this *Channel) handleConnReconnect() {
	var connected = this.conn.NotifyReconnect(make(chan bool, 1))
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
	var closed = this.channel.NotifyClose(make(chan *amqp.Error, 1))
	var cancelled = this.channel.NotifyCancel(make(chan string, 1))

	select {
	case err := <-closed:
		if err != nil {
			this.reconnect(this.config.ReconnectInterval)
		}
	case _ = <-cancelled:
		this.reconnect(this.config.ReconnectInterval)
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

		for _, c := range this.reconnects {
			c <- true
		}
		this.mu.Unlock()
	})
	this.mu.Unlock()
}

func (this *Channel) NotifyReconnect(c chan bool) chan bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		close(c)
	} else {
		this.reconnects = append(this.reconnects, c)
	}
	return c
}

func (this *Channel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.NotifyClose(c)
}

func (this *Channel) NotifyFlow(c chan bool) chan bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.NotifyFlow(c)
}

func (this *Channel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.NotifyReturn(c)
}

func (this *Channel) NotifyCancel(c chan string) chan string {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.NotifyCancel(c)
}

func (this *Channel) NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.NotifyConfirm(ack, nack)
}

func (this *Channel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.NotifyPublish(confirm)
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
// immediate - 如果为 true，当 exchange 发送消息到队列后发现队列上没有消费者，则会把消息返还给发送者
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
	return this.PublishWithDeferredConfirm(exchange, key, mandatory, immediate, msg)
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
	return this.channel.Flow(active)
}

func (this *Channel) Confirm(noWait bool) error {
	this.mu.Lock()
	defer this.mu.Unlock()
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
