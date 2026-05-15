package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	amqp.Config
	ReconnectInterval time.Duration
}
