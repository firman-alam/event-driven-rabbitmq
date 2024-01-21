package internal

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	// The connection used by the client
	conn *amqp.Connection
	// Channel is used to process / send messages
	ch *amqp.Channel
}

func ConnectRabbitMQ(username, password, host, vhost string) (*amqp.Connection, error) {
	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))
}

func NewRabbitMQClient(conn *amqp.Connection) (RabbitClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		return RabbitClient{}, err
	}

	return RabbitClient{
		conn: conn,
		ch: ch,
	}, nil
}

func (rc RabbitClient) Close() error {
	return rc.ch.Close()
}

// CreateQueue will create a new queue based on given cfgs
func (rc RabbitClient) CreateQueue(queueName string, durable, autodelete bool) (error) {
	_, err := rc.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	return err
}

// CreateBinding will bind the current channel to given exchange using the routingkey provided
func (rc RabbitClient) CreateBinding(name, binding, exchange string) (error) {
	// leaving nowait false, having nowait set to false will make the channel return an error if its fail to bind
	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

// Send is used to publish payloads onto an exchange with the given routingKey
func (rc RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	return rc.ch.PublishWithContext(
		ctx, 
		exchange, 
		routingKey, 
		true, // Mandatory is used to determine if an error should be returned upon failure
		false, // immediate
		options,
	)
}