package view

import (
	"context"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/lovoo/goka"
)

// NewBadWords - Создаем View для чтения групповой таблицы (такую возможность работы с View узнал у ИИ)
// Это чтобы можно было в методах обработчиках получать доступ к значению карты запрещенных слов, что сохраняем в персистентной таблице
func NewBadWords(ctx context.Context, codec goka.Codec, config config.Config, logger *logger.Logger) (*goka.View, error) {
	table := goka.Table(config.Processor.GroupCensorWord + "-table")
	viewBabWords, err := goka.NewView(
		config.Brokers,
		table,
		codec,
	)
	if err != nil {
		return nil, err
	}

	// Запускаем View в отдельной горутине, чтобы view.Run не блокировал следующий код
	go func() {
		logger.Info("Starting viewBabWords...")
		if err := viewBabWords.Run(ctx); err != nil {
			logger.Error("viewBabWords error: %v", err)
		}
	}()

	// ждем, когда view для запрещенных слов будет готов к работе
	select {
	case <-viewBabWords.WaitRunning():
		return viewBabWords, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
