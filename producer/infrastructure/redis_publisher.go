package infrastructure

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type RedisPublisher struct {
	client       *redis.Client
	queueName    string
	queueMaxSize int64
}

func NewRedisPublisher(redisUrl string, queueName string, queueMaxSize int64) (*RedisPublisher, error) {
	options, err := redis.ParseURL(redisUrl)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse Redis URL")
	}

	return &RedisPublisher{
		client:       redis.NewClient(options),
		queueName:    queueName,
		queueMaxSize: queueMaxSize,
	}, nil
}

func (p *RedisPublisher) Publish(ctx context.Context, bytes []byte) error {
	if err := p.client.RPush(ctx, p.queueName, bytes).Err(); err != nil {
		return errors.Wrap(err, "failed to push to tasks queue")
	}

	if err := p.client.LTrim(ctx, p.queueName, 0, p.queueMaxSize).Err(); err != nil {
		return errors.Wrap(err, "failed to trim tasks queue")
	}

	return nil
}

func (p *RedisPublisher) Close() {
	if err := p.client.Close(); err != nil {
		log.WithError(err).Error("failed to close Redis client")
	}
}
