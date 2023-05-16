package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smartwalle/rabbitmq"
	"time"
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

	setup(channel)

	go func() {
		var reconnect = channel.NotifyReconnect(make(chan bool, 1))
		for {
			select {
			case _, ok := <-reconnect:
				if !ok {
					return
				}
				fmt.Println("Channel 重连成功:", time.Now().Unix())
				setup(channel)
			}
		}
	}()

	go func() {
		var reconnect = conn.NotifyReconnect(make(chan bool, 1))
		for {
			select {
			case _, ok := <-reconnect:
				if !ok {
					return
				}
				fmt.Println("Connection 重连成功:", time.Now().Unix())
			}
		}
	}()

	var i = 0
	for {
		i++
		err = channel.Publish("", queue.Name, true, false, amqp.Publishing{
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

func setup(channel *rabbitmq.Channel) {
	channel.Confirm(false)
	go func() {
		var pChan = channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		for {
			select {
			case c, ok := <-pChan:
				if !ok {
					return
				}
				fmt.Println("发送消息结果:", c.Ack, c.DeliveryTag)
			}
		}
	}()
}
