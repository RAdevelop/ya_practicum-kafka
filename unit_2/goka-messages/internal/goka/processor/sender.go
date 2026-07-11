package processor

import (
	"context"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/lovoo/goka"
)

// MessageSender - реализует отправку сообщений, которые готовы для получения пользователями
type MessageSender struct {
	logger *logger.Logger
}

func NewMessageSender() *MessageSender {
	return &MessageSender{
		logger: logger.New("[MessageSendProcessor]"),
	}
}

func (ms MessageSender) Send(context context.Context, config config.Config, codec goka.Codec) {

	group := goka.DefineGroup(config.Processor.GroupSender,
		goka.Input(config.Topic.FilteredMessages, codec, ms.messageProcess),
	)

	processor, err := goka.NewProcessor(config.Brokers, group)
	if err != nil {
		ms.logger.Error("failed create MessageSendProcessor: %v", err)
		return
	}
	defer processor.Stop()

	if err = processor.Run(context); err != nil {
		ms.logger.Error("failed run: %v", err)
	}
}

func (ms MessageSender) messageProcess(ctx goka.Context, msg any) {
	message, ok := msg.(model.Message)
	if !ok {
		ms.logger.Error("wrong message type: %T\n", msg)
		return
	}

	ms.logger.Info("send message ID= %d, FromUserID = %d, ToUserID = %d", message.ID, message.FromUserID, message.ToUserID)
	ms.logger.Info("message: %#v\n", message)
}
