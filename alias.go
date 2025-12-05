package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

type Error = amqp.Error

type Return = amqp.Return

type Confirmation = amqp.Confirmation

type Blocking = amqp.Blocking

type Table = amqp.Table

func NewTable() Table {
	return make(Table)
}

type Queue = amqp.Queue

type Delivery = amqp.Delivery

type Publishing = amqp.Publishing

type DeferredConfirmation = amqp.DeferredConfirmation

const (
	ExchangeTypeDirect         = "direct"
	ExchangeTypeTopic          = "topic"
	ExchangeTypeFanout         = "fanout"
	ExchangeTypeHeaders        = "headers"
	ExchangeTypeDelayedMessage = "x-delayed-message"
)
