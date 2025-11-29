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

	queue, err := channel.QueueDeclare("simple_queue", true, true, false, false, nil)
	if err != nil {
		log.Println("创建 Queue 异常:", err)
		return
	}
	log.Println("创建 Queue 成功")

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
