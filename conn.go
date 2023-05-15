package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type Conn struct {
	mu     *sync.Mutex
	conn   *amqp.Connection
	url    string
	config Config
}

func NewConn(url string, config Config) (*Conn, error) {
	if config.ReconnectInterval <= 0 {
		config.ReconnectInterval = time.Second * 5
	}

	var nConn = &Conn{}
	nConn.mu = &sync.Mutex{}
	nConn.url = url
	nConn.config = config
	if err := nConn.connect(); err != nil {
		return nil, err
	}
	return nConn, nil
}

func (this *Conn) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.conn.Close()
}

func (this *Conn) connect() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	conn, err := amqp.DialConfig(this.url, this.config.Config)
	if err != nil {
		return err
	}
	if this.conn != nil {
		this.conn.Close()
	}
	this.conn = conn

	go this.handleNotify()

	return nil
}

func (this *Conn) handleNotify() {
	var closed = this.conn.NotifyClose(make(chan *amqp.Error, 1))
	select {
	case err := <-closed:
		if err != nil {
			fmt.Println("Conn 连接断开:", err)
			this.reconnect()
		}
	}
}

func (this *Conn) reconnect() {
	for {
		fmt.Printf("Conn 将在 %d 后重连\n", this.config.ReconnectInterval)
		time.Sleep(this.config.ReconnectInterval)
		fmt.Println("Conn 开始重连...")
		var err = this.connect()
		if err == nil {
			fmt.Println("Conn 连接成功")
			return
		} else {
			fmt.Println("Conn 连接发生错误:", err)
		}
	}
}

func (this *Conn) Channel() (*Channel, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	return newChannel(this, this.config)
}
