package view

import (
	"context"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/lovoo/goka"
)

// NewView - Создаем View для чтения групповой таблицы (такую возможность работы с View узнал у ИИ)
// Это чтобы можно было в методах обработчиках получать доступ, например, к значению карты запрещенных слов, что сохраняем в персистентной таблице
func NewView(ctx context.Context, table goka.Table, codec goka.Codec, config config.Config, logger *logger.Logger) (*goka.View, error) {
	view, err := goka.NewView(
		config.Brokers,
		table,
		codec,
	)
	if err != nil {
		return nil, err
	}

	// Запускаем View в отдельной горутине, чтобы view.Run не блокировал следующий код
	go func() {
		logger.Info("Starting view...")
		if err = view.Run(ctx); err != nil {
			logger.Error("view error: %v", err)
		}
	}()

	// ждем, когда нужный view будет готов к работе
	select {
	case <-view.WaitRunning():
		return view, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
