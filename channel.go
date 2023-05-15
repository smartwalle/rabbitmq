package rabbitmq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type Channel struct {
	mu      *sync.Mutex
	conn    *Conn
	channel *amqp.Channel
	config  Config
}

func newChannel(conn *Conn, config Config) (*Channel, error) {
	var nChannel = &Channel{}
	nChannel.mu = &sync.Mutex{}
	nChannel.conn = conn
	nChannel.config = config
	if err := nChannel.connect(); err != nil {
		return nil, err
	}
	return nChannel, nil
}

func (this *Channel) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Close()
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
			fmt.Println("Channel 断开:", err)
			this.reconnect()
		}

	case err := <-cancelled:
		fmt.Println("Channel Cancelled:", err)
		this.reconnect()
	}
}

func (this *Channel) reconnect() {
	for {
		fmt.Printf("Channel 将在 %d 后重连\n", this.config.ReconnectInterval)
		time.Sleep(this.config.ReconnectInterval)
		fmt.Println("Channel 开始重连...")
		var err = this.connect()
		if err == nil {
			fmt.Println("Channel 连接成功")
			return
		} else {
			fmt.Println("Channel 连接发生错误:", err)
		}
	}
}

func (this *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (this *Channel) QueueDeclarePassive(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}

func (this *Channel) QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (this *Channel) ExchangeDeclarePassive(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}

func (this *Channel) ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (this *Channel) QueueBind(name string, key string, exchange string, noWait bool, args amqp.Table) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.QueueBind(name, key, exchange, noWait, args)
}

func (this *Channel) Qos(prefetchCount int, prefetchSize int, global bool) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Qos(prefetchCount, prefetchSize, global)
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

func (this *Channel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (this *Channel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.NotifyReturn(c)
}

func (this *Channel) Confirm(noWait bool) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.Confirm(noWait)
}

func (this *Channel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.NotifyPublish(confirm)
}

func (this *Channel) NotifyFlow(c chan bool) chan bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.channel.NotifyFlow(c)
}
