package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type Config struct {
	amqp.Config
	ReconnectInterval time.Duration // 重连间隔
}
