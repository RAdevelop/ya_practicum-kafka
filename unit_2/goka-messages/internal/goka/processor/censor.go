package processor

import (
	"context"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/lovoo/goka"
)

type Censor struct {
	logger *logger.Logger
	config config.Config
}

func NewCensor(config config.Config) *Censor {
	return &Censor{
		logger: logger.New("[CensorProcessor]"),
		config: config,
	}
}

func (c Censor) Cens(context context.Context, config config.Config, codec goka.Codec) {
	group := goka.DefineGroup(goka.Group(config.Processor.GroupCensorWord),
		goka.Input(goka.Stream(config.Topic.Messages), codec, c.process),
		goka.Output(goka.Stream(config.Topic.FilteredMessages), codec),
	)

	processor, err := goka.NewProcessor(config.Processor.Brokers, group)
	if err != nil {
		c.logger.Error("create processor error: %s", err)
		return
	}
	defer processor.Stop()

	if err = processor.Run(context); err != nil {
		c.logger.Error("run processor error: %s", err)
	}
}

func (c Censor) process(context goka.Context, msg any) {
	message, ok := msg.(model.Message)
	if !ok {
		c.logger.Error("wrong message type: %T\n", msg)
		return
	}

	c.logger.Info("process messageID= %s, %#v", message.IDToString(), msg)
	context.Emit(goka.Stream(c.config.Topic.FilteredMessages), message.IDToString(), msg)
}
