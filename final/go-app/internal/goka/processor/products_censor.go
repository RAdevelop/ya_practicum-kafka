package processor

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/api"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/cert"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/store"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/models"
	"github.com/lovoo/goka"
)

type ProductsCensor struct {
	logger        *logger.Logger
	config        config.Config
	views         *api.Views
	codecProducts *jsCodec.JsonCodec[models.Product]
}

func NewProductsCensor(config config.Config, views *api.Views, codecProducts *jsCodec.JsonCodec[models.Product]) *ProductsCensor {
	return &ProductsCensor{
		logger:        logger.New("[ProcessorProductsCensor]"),
		config:        config,
		views:         views,
		codecProducts: codecProducts,
	}
}

// Run - запуск процесс применения цензуры
func (c *ProductsCensor) Run(ctx context.Context) {

	// TLS-конфиг
	tlsConfig, err := cert.LoadTLSConfig(
		c.config.Shop.SslCaLocation,
		c.config.Shop.SslCertLocation,
		c.config.Shop.SslCertificatePK8,
	)
	if err != nil {
		c.logger.Error("Failed to load TLS config: %v", err)
		return
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = tlsConfig
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
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

	// определяем группу для цензуры
	group := goka.DefineGroup(c.config.Processor.GroupProductsCensor,
		goka.Input(goka.Stream(c.config.Topics.Products), c.codecProducts, c.processCensForProducts),
		goka.Output(goka.Stream(c.config.Topics.ProductsPublished), c.codecProducts),
	)

	brokers := strings.Split(c.config.BootstrapServers, ",")

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
	}
	defer p.Stop()

	c.logger.Info("Starting processor...")
	if err = p.Run(ctx); err != nil {
		c.logger.Info("Processor error: %v", err)
	}
}

// processCensForProducts - фильтруем товары, не пускам заблокированные
func (c *ProductsCensor) processCensForProducts(ctx goka.Context, msg any) {

	product, ok := msg.(models.Product)
	if !ok {
		c.logger.Error("wrong product type: %T\n", msg)
		// наверное, тут надо складывать такие сообщения в DQL-топик
		return
	}

	blockedProducts, err := c.views.BlockedProductsView.Get(c.config.KeyTopic.ProductsBlocked)
	if err != nil {
		c.logger.Error("failed to get blocked products, err: %v", err)
		c.logger.Success("product has been published: %s", product.Name)
		ctx.Emit(goka.Stream(c.config.Topics.ProductsPublished), product.ProductId, product)
		return
	}

	var productsBlockedStoreStore *store.ProductsBlockedStore
	productsBlockedStoreStore, ok = blockedProducts.(*store.ProductsBlockedStore)
	if !ok {
		c.logger.Error("can't get productsBlockedStoreStore")

		c.logger.Success("product has been published: %s", product.Name)
		ctx.Emit(goka.Stream(c.config.Topics.ProductsPublished), product.ProductId, product)
		return
	}

	// Применяем цензуру

	//TODO del
	c.logger.Info("productsBlockedStoreStore.IsBlocked(product.Name): %#v", productsBlockedStoreStore.IsBlocked(product.Name))
	if productsBlockedStoreStore.IsBlocked(product.Name) {
		c.logger.Error("product is blocked by name: %s", product.Name)
		return
	}

	c.logger.Success("product has been published: %s", product.Name)
	ctx.Emit(goka.Stream(c.config.Topics.ProductsPublished), product.ProductId, product)
}
