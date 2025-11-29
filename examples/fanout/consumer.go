package main

import (
	"github.com/smartwalle/rabbitmq"
	"log"
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

	var exchange = "exchange_fanout"

	if err = channel.ExchangeDeclare(exchange, rabbitmq.ExchangeTypeFanout, false, false, false, false, nil); err != nil {
		log.Println("创建 Exchange 异常:", err)
		return
	}

	queue, err := channel.QueueDeclare("", true, true, false, false, nil)
	if err != nil {
		log.Println("创建 Queue 异常:", err)
		return
	}
	if err = channel.QueueBind(queue.Name, "", exchange, false, nil); err != nil {
		log.Println("Queue 绑定 Exchange 异常:", err)
		return
	}

	messages, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Println("接收消息异常:", err)
		return
	}
	for {
		select {
		case message, ok := <-messages:
			if !ok {
				return
			}
			log.Println("收到消息:", string(message.Body))
			message.Ack(false)
		}
	}
}
