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

	var exchange = "exchange_topic"

	if err = channel.ExchangeDeclare(exchange, rabbitmq.ExchangeTypeTopic, false, false, false, false, nil); err != nil {
		log.Println("创建 Exchange 异常:", err)
		return
	}

	var pushMessage = func(idx int, key, body string) {
		err = channel.Publish(exchange, key, true, false, rabbitmq.Publishing{
			Body: []byte(body),
		})

		if err != nil {
			log.Printf("发送消息 %d 异常: %+v \n", idx, err)
		} else {
			log.Printf("发送消息 %d 成功 \n", idx)
		}
	}

	var idx = 0
	for {
		idx++

		pushMessage(idx, "key.1", fmt.Sprintf("%s-%d-key.1", exchange, idx))
		pushMessage(idx, "key.1.1", fmt.Sprintf("%s-%d-key.1.1", exchange, idx))

		time.Sleep(time.Second)
	}
}
