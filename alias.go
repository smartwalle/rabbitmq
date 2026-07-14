package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Config = amqp.Config
type Recovery = amqp.Recovery
type ReconnectionConfig = amqp.ReconnectionConfig

const (
	ContentTooLarge    = amqp.ContentTooLarge
	NoRoute            = amqp.NoRoute
	NoConsumers        = amqp.NoConsumers
	ConnectionForced   = amqp.ConnectionForced
	InvalidPath        = amqp.InvalidPath
	AccessRefused      = amqp.AccessRefused
	NotFound           = amqp.NotFound
	ResourceLocked     = amqp.ResourceLocked
	PreconditionFailed = amqp.PreconditionFailed
	FrameError         = amqp.FrameError
	SyntaxError        = amqp.SyntaxError
	CommandInvalid     = amqp.CommandInvalid
	ChannelError       = amqp.ChannelError
	UnexpectedFrame    = amqp.UnexpectedFrame
	ResourceError      = amqp.ResourceError
	NotAllowed         = amqp.NotAllowed
	NotImplemented     = amqp.NotImplemented
	InternalError      = amqp.InternalError
)

type Table = amqp.Table

func NewTable() Table {
	return make(Table)
}

type Queue = amqp.Queue

type Publishing = amqp.Publishing

type DeferredConfirmation = amqp.DeferredConfirmation

type StateChanged = amqp.StateChanged

type Message = amqp.Delivery

const (
	ExchangeTypeDirect         = "direct"
	ExchangeTypeTopic          = "topic"
	ExchangeTypeFanout         = "fanout"
	ExchangeTypeHeaders        = "headers"
	ExchangeTypeDelayedMessage = "x-delayed-message"
)

const (
	Transient  = amqp.Transient
	Persistent = amqp.Persistent
)

type LifeCycleState = amqp.LifeCycleState

const (
	StateOpen         = amqp.StateOpen
	StateReconnecting = amqp.StateReconnecting
	StateClosing      = amqp.StateClosing
	StateClosed       = amqp.StateClosed
)

type StateChangedHandler func(state *StateChanged)
type CancelHandler func(consumerTag string)
type MessageHandler func(ctx context.Context, msg Message)
