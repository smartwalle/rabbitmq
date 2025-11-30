package main

import (
	"fmt"
	"github.com/smartwalle/rabbitmq"
	"log"
	"time"
)

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	conn, err := rabbitmq.NewConn("amqp://admin:admin@localhost", rabbitmq.Config{})
	if err != nil {
		log.Println("连接 RabbitMQ 异常:", err)
		return
	}
	defer conn.Close()
	log.Println("连接 RabbitMQ 成功")

	channel, err := conn.Channel()
	if err != nil {
		log.Println("创建 Channel 异常:", err)
		return
	}
	defer channel.Close()
	log.Println("创建 Channel 成功")

	var exchange = "exchange_delayed_message"

	var args = rabbitmq.NewTable()
	args["x-delayed-type"] = "direct"

	if err = channel.ExchangeDeclare(exchange, rabbitmq.ExchangeTypeDelayedMessage, true, false, false, false, args); err != nil {
		log.Println("创建 Exchange 异常:", err)
		return
	}

	var idx = 0
	for {
		idx++

		var header = rabbitmq.NewTable()
		header["x-delay"] = 10000

		err = channel.Publish(exchange, "", true, false, rabbitmq.Publishing{
			Headers: header,
			Body:    []byte(fmt.Sprintf("%s-%d", exchange, idx)),
		})

		if err != nil {
			log.Printf("发送消息 %d 异常: %+v \n", idx, err)
		} else {
			log.Printf("发送消息 %d 成功 \n", idx)
		}

		time.Sleep(time.Second)
	}
}
