package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

type Channel interface {
	// Qos 设置当前 Channel 的 prefetch 限制。
	//
	// prefetchCount - 未 ack 消息数量限制
	//
	// prefetchSize - 未 ack 消息大小限制
	//
	// global - 是否应用到当前 Channel 上的所有 consumer
	Qos(prefetchCount, prefetchSize int, global bool) error

	// ExchangeDeclare 声明 exchange。
	//
	// name - exchange 名称
	//
	// kind - exchange 类型
	//
	// durable - 是否持久化
	//
	// autoDelete - 是否自动删除
	//
	// internal - 是否为内部 exchange
	//
	// noWait - 是否不等待服务端确认
	//
	// args - 其它参数
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args Table) error

	// ExchangeDeclarePassive 被动声明 exchange，用于检查 exchange 是否存在。
	//
	// name - exchange 名称
	//
	// kind - exchange 类型
	//
	// durable - 是否持久化
	//
	// autoDelete - 是否自动删除
	//
	// internal - 是否为内部 exchange
	//
	// noWait - 是否不等待服务端确认
	//
	// args - 其它参数
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args Table) error

	// ExchangeDelete 删除 exchange。
	//
	// name - exchange 名称
	//
	// ifUnused - 是否仅在未使用时删除
	//
	// noWait - 是否不等待服务端确认
	ExchangeDelete(name string, ifUnused, noWait bool) error

	// ExchangeBind 绑定两个 exchange。
	//
	// destination - 目标 exchange 名称
	//
	// key - routing key
	//
	// source - 来源 exchange 名称
	//
	// noWait - 是否不等待服务端确认
	//
	// args - 其它参数
	ExchangeBind(destination, key, source string, noWait bool, args Table) error

	// ExchangeUnbind 解绑两个 exchange。
	//
	// destination - 目标 exchange 名称
	//
	// key - routing key
	//
	// source - 来源 exchange 名称
	//
	// noWait - 是否不等待服务端确认
	//
	// args - 其它参数
	ExchangeUnbind(destination, key, source string, noWait bool, args Table) error

	// QueueDeclare 声明 queue。
	//
	// name - 队列名称
	// 第一版不支持匿名队列，name 为空时返回 ErrAnonymousQueue。
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
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args Table) (Queue, error)

	// QueueDeclarePassive 被动声明 queue，用于检查 queue 是否存在。
	//
	// name - 队列名称
	// 第一版不支持匿名队列，name 为空时返回 ErrAnonymousQueue。
	//
	// durable - 是否持久化
	//
	// autoDelete - 是否自动删除
	//
	// exclusive - 是否独占
	//
	// noWait - 是否不等待服务端确认
	//
	// args - 其它参数
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args Table) (Queue, error)

	// QueueBind 绑定 queue 到 exchange。
	//
	// name - 队列名称
	//
	// key - routing key
	//
	// exchange - exchange 名称
	//
	// noWait - 是否不等待服务端确认
	//
	// args - 其它参数
	QueueBind(name, key, exchange string, noWait bool, args Table) error

	// QueueUnbind 解绑 queue 和 exchange。
	//
	// name - 队列名称
	//
	// key - routing key
	//
	// exchange - exchange 名称
	//
	// args - 其它参数
	QueueUnbind(name, key, exchange string, args Table) error

	// QueueDelete 删除 queue。
	//
	// name - 队列名称
	//
	// ifUnused - 是否仅在未使用时删除
	//
	// ifEmpty - 是否仅在为空时删除
	//
	// noWait - 是否不等待服务端确认
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)

	// QueuePurge 清空 queue 中的消息。
	//
	// name - 队列名称
	//
	// noWait - 是否不等待服务端确认
	QueuePurge(name string, noWait bool) (int, error)

	// Close 关闭当前 Channel。
	//
	// 只有显式通过 Conn.Channel() 获取并由调用方持有的 Channel 才应由调用方关闭。
	// Producer.Channel() 和 Consumer.Channel() 返回的 Channel 由包装层管理，业务不能主动调用 Close。
	Close() error
}

type channel struct {
	*amqp.Channel
	closable bool
}

func (c *channel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return c.Channel.Qos(prefetchCount, prefetchSize, global)
}

func (c *channel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args Table) error {
	return c.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c *channel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args Table) error {
	return c.Channel.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c *channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return c.Channel.ExchangeDelete(name, ifUnused, noWait)
}

func (c *channel) ExchangeBind(destination, key, source string, noWait bool, args Table) error {
	return c.Channel.ExchangeBind(destination, key, source, noWait, args)
}

func (c *channel) ExchangeUnbind(destination, key, source string, noWait bool, args Table) error {
	return c.Channel.ExchangeUnbind(destination, key, source, noWait, args)
}

func (c *channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args Table) (Queue, error) {
	if name == "" {
		return Queue{}, ErrAnonymousQueue
	}
	return c.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (c *channel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args Table) (Queue, error) {
	if name == "" {
		return Queue{}, ErrAnonymousQueue
	}
	return c.Channel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}

func (c *channel) QueueBind(name, key, exchange string, noWait bool, args Table) error {
	return c.Channel.QueueBind(name, key, exchange, noWait, args)
}

func (c *channel) QueueUnbind(name, key, exchange string, args Table) error {
	return c.Channel.QueueUnbind(name, key, exchange, args)
}

func (c *channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	return c.Channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (c *channel) QueuePurge(name string, noWait bool) (int, error) {
	return c.Channel.QueuePurge(name, noWait)
}

func (c *channel) Close() error {
	if !c.closable {
		return ErrChannelCloseForbidden
	}
	return c.close()
}

func (c *channel) close() error {
	return c.Channel.Close()
}
