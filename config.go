package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type Config struct {
	amqp.Config
	ConnectionReconnectInterval time.Duration
	ChannelReconnectInterval    time.Duration
}
