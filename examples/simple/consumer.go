package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/smartwalle/rabbitmq"
	"github.com/smartwalle/rabbitmq/examples"
)

func main() {
	conn, err := rabbitmq.New(examples.URL, rabbitmq.Config{
		Recovery: &rabbitmq.Recovery{
			ReconnectionConfig: &rabbitmq.ReconnectionConfig{
				MaxRetryCount: 100,
				RetryInterval: time.Second,
				RecoverableErrorCodes: []int{
					rabbitmq.ContentTooLarge,
					rabbitmq.NoRoute,
					rabbitmq.NoConsumers,
					rabbitmq.ConnectionForced,
					rabbitmq.InvalidPath,
					rabbitmq.AccessRefused,
					rabbitmq.NotFound,
					rabbitmq.ResourceLocked,
					rabbitmq.PreconditionFailed,
					rabbitmq.FrameError,
					rabbitmq.SyntaxError,
					rabbitmq.CommandInvalid,
					rabbitmq.ChannelError,
					rabbitmq.UnexpectedFrame,
					rabbitmq.ResourceError,
					rabbitmq.NotAllowed,
					rabbitmq.NotImplemented,
					rabbitmq.InternalError,
				},
			},
		},
	})
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	conn.OnStateChanged(func(state *rabbitmq.StateChanged) {
		fmt.Println("Conn OnStateChanged", state.From, state.To, state.Err)
	})

	consumer, err := conn.Consumer("queue.Name", "", false, false, false, false, nil)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(consumer.Channel().QueueDeclare("queue.Name", true, true, false, false, nil))
	fmt.Println(consumer.Channel().Qos(2, 0, false))
	consumer.OnMessage(func(ctx context.Context, msg rabbitmq.Message) {
		go func() {
			fmt.Println("Consumer OnMessage", string(msg.Body))
			msg.Ack(false)
		}()
	})
	consumer.OnCancel(func(consumerTag string) {
		fmt.Println("Consumer OnCancel", consumerTag)
	})
	fmt.Println(consumer.Start(context.Background()))
	fmt.Println(consumer.Start(context.Background()))

	time.Sleep(time.Second)

	for {
		time.Sleep(time.Second * 1)
	}
}
