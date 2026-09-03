package iface

import (
	"context"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/logger"
)

type Consumer[T any] interface {
	Consume(context.Context, func(context.Context, *logger.Logger, []*T) error)
	Close() error
	SubscribeTopic(string) error
}
