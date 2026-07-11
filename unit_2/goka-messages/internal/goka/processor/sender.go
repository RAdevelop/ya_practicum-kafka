package processor

import (
	"context"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/lovoo/goka"
)

type MessageSender struct {
	logger *logger.Logger
}

func NewMessageSender() *MessageSender {
	return &MessageSender{
		logger: logger.New("[MessageSendProcessor]"),
	}
}

func (ms MessageSender) Send(context context.Context, config config.Config, codec goka.Codec) {

	group := goka.DefineGroup(goka.Group(config.Processor.GroupSender),
		goka.Input(goka.Stream(config.Topic.FilteredMessages), codec, ms.messageProcess),
	)

	processor, err := goka.NewProcessor(config.Processor.Brokers, group)
	if err != nil {
		ms.logger.Error("failed create MessageSendProcessor: %v", err)
		return
	}
	defer processor.Stop()

	if err = processor.Run(context); err != nil {
		ms.logger.Error("failed run: %v", err)
	}
}

func (ms MessageSender) messageProcess(context goka.Context, msg any) {
	message, ok := msg.(model.Message)
	if !ok {
		ms.logger.Error("wrong message type: %T\n", msg)
		return
	}

	ms.logger.Info("process message: %#v\n", message)
}
