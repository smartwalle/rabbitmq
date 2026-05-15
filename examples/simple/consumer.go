package main

import (
	"log"

	"github.com/smartwalle/rabbitmq"
	"github.com/smartwalle/rabbitmq/examples"
)

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	conn, err := rabbitmq.NewConn(examples.URL, rabbitmq.Config{})
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

	queue, err := channel.QueueDeclare("simple_queue", true, false, false, false, nil)
	if err != nil {
		log.Println("创建 Queue 异常:", err)
		return
	}
	log.Println("创建 Queue 成功")

	channel.Qos(2, 0, false)

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

			go func(msg rabbitmq.Delivery) {
				//time.Sleep(time.Second * 5)
				log.Println("------:", string(msg.Body))
				msg.Ack(false)
			}(message)

		}
	}
}
