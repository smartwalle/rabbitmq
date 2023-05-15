package main

import (
	"fmt"
	"github.com/smartwalle/rabbitmq"
)

func main() {
	var conn, err = rabbitmq.NewConn("amqp://guest:guest@localhost", rabbitmq.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}

	conn.Channel()

	fmt.Println(conn)

	select {}
}
