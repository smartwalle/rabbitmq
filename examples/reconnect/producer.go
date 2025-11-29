package main

import (
	"fmt"
	"github.com/smartwalle/rabbitmq"
	"time"
)

func main() {
	var conn, err = rabbitmq.NewConn("amqp://admin:admin@localhost", rabbitmq.Config{})
	if err != nil {
		fmt.Println("连接 RabbitMQ 发生错误:", err)
		return
	}
	fmt.Println("连接 RabbitMQ 成功")
	defer conn.Close()

	conn.OnClose(func(err *rabbitmq.Error) {
		fmt.Println("Conn OnClose:", err)
	})

	conn.OnReconnect(func(conn *rabbitmq.Connection) {
		fmt.Println("Conn OnReconnect:", time.Now().Unix())
	})

	channel, err := conn.Channel()
	if err != nil {
		fmt.Println("创建 Channel 发生错误:", channel)
		return
	}
	fmt.Println("创建 Channel 成功")
	defer channel.Close()

	queue, err := channel.QueueDeclare(
		"simple-queue", // name of the queue
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // noWait
		nil,            // arguments
	)
	if err != nil {
		fmt.Println("创建队列发生错误:", err)
		return
	}
	fmt.Println("创建队列成功:", queue.Name)

	channel.OnReconnect(func(channel *rabbitmq.Channel) {
		fmt.Println("Channel OnReconnect:", time.Now().Unix())
	})

	channel.OnClose(func(err *rabbitmq.Error) {
		fmt.Println("Channel OnClose:", err)
	})

	var i = 0
	for {
		i++
		err = channel.Publish("", queue.Name, true, false, rabbitmq.Publishing{
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
