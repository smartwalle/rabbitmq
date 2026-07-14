package rabbitmq

import (
	"context"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

type consumerState int32

const (
	consumerStateIdle    consumerState = 0
	consumerStateRunning consumerState = 1
	consumerStateClosed  consumerState = 2
)

type Consumer interface {
	// Channel 返回 Consumer 内部持有的消费 Channel。
	//
	// 该 Channel 只用于执行与消费 Channel 绑定的设置，例如 Qos。
	// 业务不能通过这里返回的 Channel 调用 Close，也不能保存后跨 Consumer
	// 生命周期继续使用；Consumer 的关闭必须通过 Stop 完成。
	Channel() Channel

	// OnCancel 注册 broker 主动取消 Consumer 的回调。
	//
	// 当 RabbitMQ 发送 basic.cancel 时触发，例如 queue 被删除、consumer 被服务端取消等。
	// 该回调可以重复设置，后一次覆盖前一次。回调触发后，当前 Consumer 会进入
	// closed 状态，不能再次 Start。
	OnCancel(handler CancelHandler)

	OnMessage(handler MessageHandler)

	Start(ctx context.Context) error

	Stop(ctx context.Context) error
}

type consumer struct {
	queue       string
	consumerTag string
	autoAck     bool
	exclusive   bool
	noLocal     bool
	noWait      bool
	args        amqp.Table

	mu    sync.Mutex
	state consumerState

	cancelHandler  atomic.Value
	messageHandler atomic.Value

	ch     *channel
	cancel context.CancelFunc
	done   chan struct{}
}

func (c *consumer) Channel() Channel {
	return c.ch
}

func (c *consumer) OnCancel(handler CancelHandler) {
	if handler == nil {
		return
	}
	c.cancelHandler.Store(handler)
}

func (c *consumer) OnMessage(handler MessageHandler) {
	if handler == nil {
		return
	}
	c.messageHandler.Store(handler)
}

func (c *consumer) Start(ctx context.Context) error {
	if c.queue == "" {
		return ErrAnonymousQueue
	}

	if c.messageHandlerSnapshot() == nil {
		return ErrConsumerHandlerRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ch == nil || c.ch.IsClosed() {
		return ErrConsumerClosed
	}

	switch c.state {
	case consumerStateRunning:
		return ErrConsumerRunning
	case consumerStateClosed:
		return ErrConsumerClosed
	default:
	}

	// 启动期间持有锁，Stop 需要等待 ConsumeWithContext 返回后才能继续。
	// 这是当前生命周期设计：未启动成功或启动过程中，不支持被 Stop 打断。
	runCtx, cancel := context.WithCancel(ctx)
	// 先注册 basic.cancel 监听，避免 Consume 成功后 broker 立即取消 consumer 时丢失通知。
	var cancelNotify = make(chan string, 1)
	c.ch.NotifyCancel(cancelNotify)

	var stateChangeNotify = make(chan *StateChanged, 16)
	c.ch.NotifyStateChange(stateChangeNotify)

	deliveries, err := c.ch.ConsumeWithContext(runCtx, c.queue, c.consumerTag, c.autoAck, c.exclusive, c.noLocal, c.noWait, c.args)
	if err != nil {
		c.state = consumerStateClosed
		cancel()
		_ = c.ch.close()
		return err
	}

	var done = make(chan struct{})

	c.cancel = cancel
	c.done = done
	c.state = consumerStateRunning

	go c.handleDeliveries(runCtx, deliveries, stateChangeNotify, cancelNotify, done)
	return nil
}

func (c *consumer) handleDeliveries(ctx context.Context, deliveries <-chan amqp.Delivery, stateChangeNotify <-chan *StateChanged, cancelNotify <-chan string, done chan struct{}) {
	var brokerCancelledTag string

	defer func() {
		c.mu.Lock()
		if c.state != consumerStateClosed {
			c.state = consumerStateClosed
		}
		cancel := c.cancel
		ch := c.ch
		c.mu.Unlock()

		if ch != nil {
			_ = ch.close()
		}

		if cancel != nil {
			cancel()
		}

		close(done)

		if brokerCancelledTag != "" {
			c.handleCancel(brokerCancelledTag)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case consumerTag, ok := <-cancelNotify:
			if !ok {
				// cancelNotify 关闭只表示不会再收到 broker cancel 通知，不代表 deliveries 已结束。
				cancelNotify = nil
				continue
			}
			brokerCancelledTag = consumerTag
			return
		case _, ok := <-stateChangeNotify:
			if !ok {
				// stateChangeNotify 关闭表示后续不再接收 channel 状态变化，不代表 deliveries 已结束。
				stateChangeNotify = nil
				continue
			}
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}
			c.handleMessage(ctx, delivery)
		}
	}
}

func (c *consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.state == consumerStateClosed {
		c.mu.Unlock()
		return nil
	}

	c.state = consumerStateClosed
	cancel := c.cancel
	done := c.done
	ch := c.ch
	c.mu.Unlock()

	var err error
	if ch != nil && !ch.IsClosed() {
		err = ch.close()
	}

	if cancel != nil {
		cancel()
	}

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
			}
		}
	}

	return err
}

func (c *consumer) handleCancel(consumerTag string) {
	var handler = c.cancelHandlerSnapshot()
	if handler == nil {
		return
	}

	defer func() {
		_ = recover()
	}()

	handler(consumerTag)
}

func (c *consumer) handleMessage(ctx context.Context, msg Message) {
	var handler = c.messageHandlerSnapshot()
	if handler == nil {
		return
	}
	handler(ctx, msg)
}

func (c *consumer) cancelHandlerSnapshot() CancelHandler {
	handler, _ := c.cancelHandler.Load().(CancelHandler)
	return handler
}

func (c *consumer) messageHandlerSnapshot() MessageHandler {
	handler, _ := c.messageHandler.Load().(MessageHandler)
	return handler
}
