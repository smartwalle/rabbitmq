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

	queue, err := channel.QueueDeclare("simple_queue", true, true, false, false, nil)
	if err != nil {
		log.Println("创建 Queue 异常:", err)
		return
	}
	log.Println("创建 Queue 成功")

	var i = 0
	for {
		i++
		err = channel.Publish("", queue.Name, false, false, rabbitmq.Publishing{
			Body: []byte(fmt.Sprintf("hello %d", i)),
		})
		if err != nil {
			fmt.Printf("发送消息 %d 发生错误: %v \n", i, err)
		} else {
			fmt.Printf("发送消息 %d 成功 \n", i)
		}

		time.Sleep(time.Second)
	}

	select {}
}
