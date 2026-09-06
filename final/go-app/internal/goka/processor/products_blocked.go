package processor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
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
}

func NewProductsBlocked(config config.Config) *ProductsBlocked {
	return &ProductsBlocked{
		logger: logger.New("[ProductsBlockedProcessor]"),
		config: config,
	}
}

// Run - запуск процесса актуализации списка заблокированных товаров
func (pb *ProductsBlocked) Run(ctx context.Context) {
	codecProductsBlocked := new(jsCodec.JsonCodec[store.ProductsBlockedStore])

	// определяем группу для заблокированных товаров
	group := goka.DefineGroup(pb.config.Processor.GroupProductsBlocked,
		goka.Input(goka.Stream(pb.config.Topics.ProductsBlocked), new(codec.String), pb.productsBlockedUpdate),
		goka.Persist(codecProductsBlocked),
	)

	// TLS-конфиг
	tlsConfig, err := loadTLSConfig(
		pb.config.Shop.SslCaLocation,
		pb.config.Shop.SslCertLocation,
		pb.config.Shop.SslCertificatePK8,
	)
	if err != nil {
		pb.logger.Error("Failed to load TLS config: %v", err)
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
		if len(brokers) == 0 {
			return nil, fmt.Errorf("brokers list is empty")
		}
		for i, b := range brokers {
			if strings.TrimSpace(b) == "" {
				return nil, fmt.Errorf("broker at index %d is empty", i)
			}
		}

		tm, err := goka.NewTopicManager(brokers, saramaConfig, topicManagerConfig)
		if err != nil {
			log.Printf("❌ NewTopicManager error: %v", err)
			return nil, err
		}

		// Важно: попробуй сразу вызвать EnsureTableExists, чтобы проверить, что TM живой
		// Но осторожно: это может создать топики раньше времени. Для отладки можно.
		log.Println("✅ TopicManager created successfully")
		return tm, nil
	}

	brokers := strings.Split(pb.config.BootstrapServers, ",")

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

	pb.logger.Info("Starting processor...")
	if err = p.Run(ctx); err != nil {
		pb.logger.Info("Processor error: %v", err)
	}
}

func (pb *ProductsBlocked) productsBlockedUpdate(ctx goka.Context, msg any) {

	productName, ok := msg.(string)
	if !ok {
		pb.logger.Error("productsBlocked update: message is not a string: %T", msg)
		return
	}
	// Нормализуем имя
	productName = strings.ToLower(strings.TrimSpace(productName))
	if productName == "" {
		return
	}

	var productsBlockedStore store.ProductsBlockedStore
	if val := ctx.Value(); val != nil {
		productsBlockedStore, ok = val.(store.ProductsBlockedStore)
		if !ok {
			pb.logger.Error("wrong store type: %T", val)
		}
	}

	productsBlockedStore.Add(productName)
	ctx.SetValue(productsBlockedStore)
	pb.logger.Success("productsBlocked updated: %#v", productsBlockedStore)
}

func loadTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("parse CA: %w", err)
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}, nil
}
