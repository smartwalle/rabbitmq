package rabbitmq

import "errors"

var (
	ErrConnClosed   = errors.New("rabbitmq connection closed")
	ErrNotConnected = errors.New("rabbitmq not connected")

	ErrConsumerRunning         = errors.New("consumer is running")
	ErrConsumerClosed          = errors.New("consumer closed")
	ErrConsumerHandlerRequired = errors.New("consumer handler required")
	ErrChannelCloseForbidden   = errors.New("rabbitmq channel close forbidden")

	ErrPublishUnconfirmed = errors.New("rabbitmq publish unconfirmed")
	ErrPublishNacked      = errors.New("rabbitmq publish nacked")
	ErrAnonymousQueue     = errors.New("rabbitmq anonymous queue unsupported")
)
