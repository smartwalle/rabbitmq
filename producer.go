package rabbitmq

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer interface {
	Channel() Channel

	// PublishWithContext
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
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error

	Close() error
}

type producer struct {
	mu      sync.Mutex
	ch      *channel
	confirm bool
	closed  bool
}

func (p *producer) Channel() Channel {
	return p.ch
}

func (p *producer) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrConnClosed
	}
	if !p.confirm {
		return p.ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
	}

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		return err
	}
	if confirmation == nil {
		return ErrPublishUnconfirmed
	}

	ack, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ack {
		if p.ch.IsClosed() {
			return ErrPublishUnconfirmed
		}
		return ErrPublishNacked
	}

	return nil
}

func (p *producer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	ch := p.ch
	p.mu.Unlock()

	var err error
	if ch != nil {
		err = ch.close()
	}

	return err
}
