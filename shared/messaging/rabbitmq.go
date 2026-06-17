package messaging

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	conn    *amqp.Connection
	Channel *amqp.Channel
}

func NewRabbitMQ(uri string) (*RabbitMQ, error) {
	const retryDelay = 5 * time.Second

	var (
		conn *amqp.Connection
		err  error
	)

	for {
		conn, err = amqp.Dial(uri)
		if err == nil {
			break
		}

		log.Printf("Waiting for RabbitMQ: %v", err)
		time.Sleep(retryDelay)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	rmq := &RabbitMQ{
		conn:    conn,
		Channel: ch,
	}

	if err := rmq.setupExchangesAndQueues(); err != nil {
		rmq.Close()
		return nil, fmt.Errorf("failed to setup exchanges and queues: %w", err)
	}

	log.Println("Connected to RabbitMQ")

	return rmq, nil
}

func (r *RabbitMQ) PublishMessage(
	ctx context.Context,
	routingKey string,
	message string,
) error {
	return r.Channel.PublishWithContext(
		ctx,
		"",         // default exchange
		routingKey, // queue name
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
}

func (r *RabbitMQ) setupExchangesAndQueues() error {
	_, err := r.Channel.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // auto-delete
		false,   // exclusive
		false,   // no-wait
		nil,
	)

	return err
}

func (r *RabbitMQ) Close() {
	if r.Channel != nil {
		_ = r.Channel.Close()
	}

	if r.conn != nil {
		_ = r.conn.Close()
	}
}