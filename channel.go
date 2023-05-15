package rabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"sync/atomic"
	"time"
)

type Channel struct {
	mu       sync.Mutex
	notifyMu sync.Mutex
	conn     *Conn
	channel  *amqp.Channel
	config   Config

	noNotify        bool
	reconnects      []chan bool
	reconnectStatus int32 // 0 - 没有重连任务；1 - 有重连任务在进行中；
}

func newChannel(conn *Conn, config Config) (*Channel, error) {
	var nChannel = &Channel{}
	nChannel.conn = conn
	nChannel.config = config
	if err := nChannel.connect(); err != nil {
		return nil, err
	}
	return nChannel, nil
}

func (this *Channel) Close() error {
	this.notifyMu.Lock()
	this.noNotify = true
	for _, c := range this.reconnects {
		close(c)
	}
	this.notifyMu.Unlock()

	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Close()
}

func (this *Channel) IsClosed() bool {
	return this.channel.IsClosed()
}

func (this *Channel) connect() error {
	this.mu.Lock()
	defer this.mu.Unlock()

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
			this.reconnect()
		}
	case _ = <-cancelled:
		this.reconnect()
	}
}

func (this *Channel) reconnect() {
	if !atomic.CompareAndSwapInt32(&this.reconnectStatus, 0, 1) {
		return
	}

	for {
		time.Sleep(this.config.ReconnectInterval)
		var err = this.connect()
		if err == nil {
			atomic.StoreInt32(&this.reconnectStatus, 0)

			this.notifyMu.Lock()
			for _, c := range this.reconnects {
				c <- true
			}
			this.notifyMu.Unlock()
			return
		}
	}
}

func (this *Channel) NotifyReconnect(c chan bool) chan bool {
	this.notifyMu.Lock()
	defer this.notifyMu.Unlock()

	if this.noNotify {
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

func (this *Channel) Confirm(noWait bool) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Confirm(noWait)
}
