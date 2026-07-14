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

	producer, err := conn.Producer(true)
	if err != nil {
		log.Fatalln(err)
	}
	//fmt.Println(producer.Channel().QueueDeclare("queue.Name", true, true, false, false, nil))

	var idx = 0
	for {
		var nErr = producer.PublishWithContext(context.Background(), "xxx", "queue.Name", true, false, rabbitmq.Publishing{
			DeliveryMode: rabbitmq.Persistent,
			Body:         []byte(fmt.Sprintf("hello %d", idx)),
		})

		fmt.Println(idx, nErr)
		time.Sleep(time.Millisecond * 100)
		idx++
	}

	for {
		time.Sleep(time.Second * 1)
	}
}
