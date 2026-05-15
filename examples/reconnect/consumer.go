package main

import (
	"fmt"

	"github.com/smartwalle/rabbitmq"
	"github.com/smartwalle/rabbitmq/examples"

	"time"
)

func main() {
	var conn, err = rabbitmq.NewConn(examples.URL, rabbitmq.Config{
		ReconnectInterval: time.Second,
	})
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
		fmt.Println("创建 Channel 发生错误:", err)
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
	fmt.Println("创建队列成功")

	channel.OnReconnect(func(channel *rabbitmq.Channel) {
		fmt.Println("Channel OnReconnect:", time.Now().Unix())
	})

	channel.OnClose(func(err *rabbitmq.Error) {
		fmt.Println("Channel OnClose:", err)
	})

	go consume(channel, queue)

	time.AfterFunc(time.Second*5, func() {
		channel.Cancel("simple-consumer", true)
	})

	select {}
}

func consume(channel *rabbitmq.Channel, queue rabbitmq.Queue) {
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
