package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smartwalle/rabbitmq"
)

func main() {
	var conn, err = rabbitmq.NewConn("amqp://guest:guest@localhost", rabbitmq.Config{})
	if err != nil {
		fmt.Println("连接 RabbitMQ 发生错误:", err)
		return
	}
	fmt.Println("连接 RabbitMQ 成功")

	channel, err := conn.Channel()
	if err != nil {
		fmt.Println("创建 Channel 发生错误:", channel)
		return
	}
	fmt.Println("创建 Channel 成功")

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
	fmt.Println("创建队列成功")

	go consume(channel, queue)

	select {}
}

func consume(channel *rabbitmq.Channel, queue amqp.Queue) {
	defer func() {
		fmt.Println("停止接收消息")
	}()
	deliveries, err := channel.Consume(
		queue.Name,        // name
		"simple-consumer", // consumerTag,
		false,             // autoAck
		false,             // exclusive
		false,             // noLocal
		false,             // noWait
		nil,               // arguments
	)
	if err != nil {
		fmt.Println("接收消息发生错误:", err)
		return
	}
	fmt.Println("开始接收消息:")

	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			fmt.Println("收到消息:", string(d.Body))
			d.Ack(false)
		}
	}
}
