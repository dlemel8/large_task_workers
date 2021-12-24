package infrastructure

import (
	"context"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

const (
	queueMaxSizeKeyName = "x-max-length"
	bytesContentType    = "application/octet-stream"
)

type RabbitMqPublisher struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
}

func NewRabbitMqPublisher(rabbitmqUrl string, queueName string, queueMaxSize int64) (*RabbitMqPublisher, error) {
	connection, err := amqp.Dial(rabbitmqUrl)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open RabbitMQ connection")
	}

	channel, err := connection.Channel()
	if err != nil {
		closeConnection(connection)
		return nil, errors.Wrap(err, "failed to open RabbitMQ channel")
	}

	args := make(map[string]interface{})
	args[queueMaxSizeKeyName] = queueMaxSize
	queue, err := channel.QueueDeclare(queueName, false, false, false, false, args)
	if err != nil {
		closeChannel(channel)
		closeConnection(connection)
		return nil, errors.Wrap(err, "failed to declare RabbitMQ queue")
	}

	return &RabbitMqPublisher{
		connection: connection,
		channel:    channel,
		queue:      queue,
	}, nil
}

func (p *RabbitMqPublisher) Publish(_ context.Context, bytes []byte) error {
	message := amqp.Publishing{
		ContentType: bytesContentType,
		Body:        bytes,
	}
	return p.channel.Publish("", p.queue.Name, false, false, message)
}

func (p *RabbitMqPublisher) Close() {
	closeChannel(p.channel)
	closeConnection(p.connection)
}

func closeConnection(connection *amqp.Connection) {
	if err := connection.Close(); err != nil {
		log.WithError(err).Error("failed to close RabbitMQ connection")
	}
}

func closeChannel(channel *amqp.Channel) {
	if err := channel.Close(); err != nil {
		log.WithError(err).Error("failed to close RabbitMQ channel")
	}
}
