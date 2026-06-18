package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"ride-sharing/shared/contracts"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TripExchange = "trip"
)

type RabbitMQ struct {
	conn    *amqp.Connection
	Channel *amqp.Channel
}

type MessageHandler func(context.Context, amqp.Delivery) error

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
		_ = conn.Close()
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

func (r *RabbitMQ) ConsumeMessages(
	queueName string,
	handler MessageHandler,
) error {
	err := r.Channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	msgs, err := r.Channel.Consume(
		queueName,
		"",
		false, // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to consume messages: %w", err)
	}

	ctx := context.Background()

	go func() {
		for msg := range msgs {
			log.Printf("Received message: %s", msg.Body)

			if err := handler(ctx, msg); err != nil {
				log.Printf(
					"Failed to handle message: %v, body=%s",
					err,
					msg.Body,
				)

				if err := msg.Nack(false, false); err != nil {
					log.Printf("Failed to nack message: %v", err)
				}

				continue
			}

			if err := msg.Ack(false); err != nil {
				log.Printf(
					"Failed to ack message: %v, body=%s",
					err,
					msg.Body,
				)
			}
		}
	}()

	return nil
}

func (r *RabbitMQ) PublishMessage(ctx context.Context, routingKey string, message contracts.AmqpMessage) error {
	log.Printf(
		"Publishing message with routing key: %s",
		routingKey,
	)
	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	return r.Channel.PublishWithContext(
		ctx,
		TripExchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         jsonMsg,
			DeliveryMode: amqp.Persistent,
		},
	)
}

func (r *RabbitMQ) setupExchangesAndQueues() error {
	err := r.Channel.ExchangeDeclare(
		TripExchange, // name
		"topic",      // type
		true,         // durable
		false,        // auto-delete
		false,        // internal
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return fmt.Errorf(
			"failed to declare exchange %q: %w",
			TripExchange,
			err,
		)
	}

	if err := r.declareAndBindQueue(
		FindAvailableDriversQueue,
		[]string{
			contracts.TripEventCreated,
			contracts.TripEventDriverNotInterested,
		},
		TripExchange,
	); err != nil {
		return err
	}

	if err := r.declareAndBindQueue(
		DriverCmdTripRequestQueue,
		[]string{contracts.DriverCmdTripRequest},
		TripExchange,
	); err != nil {
		return err
	}


	return nil
}



func (r *RabbitMQ) declareAndBindQueue(
	queueName string,
	routingKeys []string,
	exchange string,
) error {
	q, err := r.Channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to declare queue %q: %w",
			queueName,
			err,
		)
	}

	for _, routingKey := range routingKeys {
		err := r.Channel.QueueBind(
			q.Name,
			routingKey,
			exchange,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to bind queue %q to routing key %q: %w",
				queueName,
				routingKey,
				err,
			)
		}
	}

	return nil
}

func (r *RabbitMQ) Close() {
	if r.Channel != nil {
		_ = r.Channel.Close()
	}

	if r.conn != nil {
		_ = r.conn.Close()
	}
}
