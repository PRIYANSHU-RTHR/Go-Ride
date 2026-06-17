package messaging

import (
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	conn *amqp.Connection
}

func connect(uri string) (*amqp.Connection, error) {
	return amqp.Dial(uri)
}

func NewRabbitMQ(uri string) (*RabbitMQ, error) {
	const retryDelay = 5 * time.Second

	for {
		conn, err := connect(uri)
		if err == nil {
			return &RabbitMQ{
				conn: conn,
			}, nil
		}

		log.Printf("Waiting for RabbitMQ: %v", err)
		time.Sleep(retryDelay)
	}
}
func (r *RabbitMQ) Close() {
	if r.conn != nil {
		r.conn.Close()
	}
}
