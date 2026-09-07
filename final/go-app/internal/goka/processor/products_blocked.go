package processor

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/cert"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/store"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/logger"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

type ProductsBlocked struct {
	logger *logger.Logger
	config config.Config
	ready  chan struct{}
}

func NewProductsBlocked(config config.Config, ready chan struct{}) *ProductsBlocked {
	return &ProductsBlocked{
		logger: logger.New("[ProductsBlockedProcessor]"),
		config: config,
		ready:  ready,
	}
}

// Run - запуск процесса актуализации списка заблокированных товаров
func (pb *ProductsBlocked) Run(ctx context.Context) {

	// TLS-конфиг
	tlsConfig, err := cert.LoadTLSConfig(
		pb.config.Shop.SslCaLocation,
		pb.config.Shop.SslCertLocation,
		pb.config.Shop.SslCertificatePK8,
	)
	if err != nil {
		pb.logger.Error("Failed to load TLS config: %v", err)
		// закрываем канал при ошибке, чтобы main не висел
		if pb.ready != nil {
			close(pb.ready)
		}
		return
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = tlsConfig
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	saramaConfig.Consumer.Group.Session.Timeout = 10 * time.Second
	saramaConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	consumerBuilder := goka.ConsumerGroupBuilderWithConfig(saramaConfig)
	saramaConsumerBuilder := goka.SaramaConsumerBuilderWithConfig(saramaConfig)

	producerBuilder := goka.ProducerBuilderWithConfig(saramaConfig)

	topicManagerConfig := goka.NewTopicManagerConfig()
	topicManagerConfig.Table.Replication = 3
	topicManagerConfig.Table.CleanupPolicy = "compact"

	topicManagerConfig.Stream.Replication = 3
	topicManagerConfig.Stream.Retention = 7 * 24 * time.Hour
	topicManagerConfig.Stream.CleanupPolicy = "delete"

	topicManagerConfig.CreateTopicTimeout = 10 * time.Second
	topicManagerConfig.MismatchBehavior = goka.TMConfigMismatchBehaviorWarn
	topicManagerConfig.NoCreate = false

	// Создаём TopicManagerBuilder с TLS
	topicManagerBuilder := func(brokers []string) (goka.TopicManager, error) {
		return goka.NewTopicManager(brokers, saramaConfig, topicManagerConfig)
	}

	brokers := strings.Split(pb.config.BootstrapServers, ",")
	codecProductsBlocked := new(jsCodec.EncodingJson[*store.ProductsBlockedStore])

	// определяем группу для заблокированных товаров
	group := goka.DefineGroup(pb.config.Processor.GroupProductsBlocked,
		goka.Input(goka.Stream(pb.config.Topics.ProductsBlocked), new(codec.String), pb.productsBlockedUpdate),
		goka.Persist(codecProductsBlocked),
	)

	p, err := goka.NewProcessor(
		brokers,
		group,
		goka.WithConsumerGroupBuilder(consumerBuilder),
		goka.WithProducerBuilder(producerBuilder),
		goka.WithTopicManagerBuilder(topicManagerBuilder),
		goka.WithConsumerSaramaBuilder(saramaConsumerBuilder),
	)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
		return
	}
	defer p.Stop()

	// Сигнализируем о готовности ПОСЛЕ создания процессора (топик создан)
	if pb.ready != nil {
		close(pb.ready)
		pb.logger.Info("ProductsBlocked processor is ready (topic created)")
	}

	pb.logger.Info("Starting processor...")
	if err = p.Run(ctx); err != nil {
		pb.logger.Info("Processor error: %v", err)
	}
}

func (pb *ProductsBlocked) productsBlockedUpdate(ctx goka.Context, msg any) {

	blockEvent, correctType := msg.(string)
	if !correctType {
		pb.logger.Error("wrong message type: %T", msg)
		return
	}

	parts := strings.Split(blockEvent, ":")
	if len(parts) != 2 {
		pb.logger.Error("invalid format: %s", blockEvent)
		return
	}

	action := parts[0]
	productName := parts[1]

	// Нормализуем имя
	productName = strings.ToLower(strings.TrimSpace(productName))
	if productName == "" {
		return
	}

	// Читаем текущее состояние
	var productsBlockedStore *store.ProductsBlockedStore
	if val := ctx.Value(); val != nil {
		var ok bool
		productsBlockedStore, ok = val.(*store.ProductsBlockedStore)
		if !ok {
			pb.logger.Error("wrong store type: %T", val)
			productsBlockedStore = &store.ProductsBlockedStore{}
		}
	} else {
		productsBlockedStore = &store.ProductsBlockedStore{}
	}

	// Обновляем список
	switch action {
	case "add":
		productsBlockedStore.Add(productName)
	case "remove":
		productsBlockedStore.Remove(productName)
	default:
		pb.logger.Error("unknown action: %s", action)
		return
	}

	ctx.SetValue(productsBlockedStore)
	pb.logger.Success("productsBlocked updated: %#v", productsBlockedStore)
}
