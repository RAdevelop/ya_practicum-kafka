package view

import (
	"context"
	"strings"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/logger"
	"github.com/lovoo/goka"
)

// NewView - Создаем View для чтения групповой таблицы
// Это чтобы можно было в методах обработчиках получать доступ, например, к значению карты запрещенных товаров, что сохраняем в персистентной таблице
func NewView(ctx context.Context, codec goka.Codec, config config.Config, logger *logger.Logger) (*goka.View, error) {

	brokers := strings.Split(config.BootstrapServers, ",")
	view, err := goka.NewView(
		brokers,
		config.ViewTable.ProductsBlocked,
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
