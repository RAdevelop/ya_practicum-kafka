package view

import (
	"context"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/logger"
	"github.com/lovoo/goka"
)

func NewViewBlockedProducts(ctx context.Context, codec goka.Codec, config config.Config, logger *logger.Logger) (*goka.View, error) {

	// TLS-конфиг
	tlsConfig, err := config.LoadShopConfigTLS()
	if err != nil {
		return nil, err
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = tlsConfig
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	saramaConsumerBuilder := goka.SaramaConsumerBuilderWithConfig(saramaConfig)

	topicManagerConfig := goka.NewTopicManagerConfig()
	topicManagerConfig.Table.Replication = 1
	topicManagerConfig.Table.CleanupPolicy = "compact"

	topicManagerConfig.CreateTopicTimeout = 10 * time.Second
	topicManagerConfig.MismatchBehavior = goka.TMConfigMismatchBehaviorWarn
	topicManagerConfig.NoCreate = false

	// Создаём TopicManagerBuilder с TLS
	topicManagerBuilder := func(brokers []string) (goka.TopicManager, error) {
		return goka.NewTopicManager(brokers, saramaConfig, topicManagerConfig)
	}

	brokers := strings.Split(config.BootstrapServers, ",")
	view, err := goka.NewView(
		brokers,
		config.ViewTable.ProductsBlocked,
		codec,
		goka.WithViewTopicManagerBuilder(topicManagerBuilder),
		goka.WithViewConsumerSaramaBuilder(saramaConsumerBuilder),
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

func NewViewRecommendationsProducts(ctx context.Context, codec goka.Codec, config config.Config, logger *logger.Logger) (*goka.View, error) {

	// TLS-конфиг
	tlsConfig, err := config.LoadClientConfigTLS()
	if err != nil {
		return nil, err
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = tlsConfig
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	saramaConsumerBuilder := goka.SaramaConsumerBuilderWithConfig(saramaConfig)

	topicManagerConfig := goka.NewTopicManagerConfig()
	topicManagerConfig.Table.Replication = 1
	topicManagerConfig.Table.CleanupPolicy = "compact"

	topicManagerConfig.CreateTopicTimeout = 10 * time.Second
	topicManagerConfig.MismatchBehavior = goka.TMConfigMismatchBehaviorWarn
	topicManagerConfig.NoCreate = false

	// Создаём TopicManagerBuilder с TLS
	topicManagerBuilder := func(brokers []string) (goka.TopicManager, error) {
		return goka.NewTopicManager(brokers, saramaConfig, topicManagerConfig)
	}

	brokers := strings.Split(config.BootstrapServers, ",")
	view, err := goka.NewView(
		brokers,
		config.ViewTable.ProductsRecommendations,
		codec,
		goka.WithViewTopicManagerBuilder(topicManagerBuilder),
		goka.WithViewConsumerSaramaBuilder(saramaConsumerBuilder),
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
