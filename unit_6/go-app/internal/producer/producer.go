package producer

import (
	"fmt"

	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/serializer"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer - продюсер для отправки сообщений в Kafka
type Producer[T any] struct {
	Logger       *logger.Logger
	producer     *kafka.Producer
	config       config.Config
	serializable serializer.Serializable[T]
}

// NewProducer - конструктор для продюсера
func NewProducer[T any](config config.Config, logger *logger.Logger, serializable serializer.Serializable[T]) (*Producer[T], error) {

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": config.Producer.BootstrapServers,
		// Гарантия At Least Once
		"acks": config.Producer.Acks, // Подтверждение от всех реплик
		// Количество повторных попыток, которые продюсер сделает, чтобы отправить сообщение, если при первой попытке произошла временная ошибка:
		"retries":            config.Producer.Retries,
		"retry.backoff.ms":   config.Producer.RetryBackoffMs,    // Пауза между попытками
		"enable.idempotence": config.Producer.EnableIdempotence, // Идемпотентность (защита от дублей)

		// Максимальное количество неподтверждённых запросов на отправку сообщений, которые продюсер может одновременно отправить на один брокер (по одному TCP-соединению), не получив ответа от брокера
		"max.in.flight.requests.per.connection": config.Producer.MaxInFlightRequestsPerConnection,
		//Определяет, сколько времени клиент (продюсер или консьюмер) будет ждать установки TCP-соединения с брокером:
		"socket.connection.setup.timeout.ms": config.Producer.SocketConnectionSetupTimeoutMs,
		// Определяет максимальное время ожидания ответа на уже отправленный запрос по уже установленному соединению:
		"socket.timeout.ms": config.Producer.SocketTimeoutMs,

		//SSL
		"security.protocol":        config.Producer.SecurityProtocol,
		"ssl.ca.location":          config.Producer.SslCaLocation,
		"ssl.certificate.location": config.Producer.SslCertificateLocation,
		"ssl.key.location":         config.Producer.SslKeyLocation,
		"ssl.key.password":         config.Producer.SslKeyPassword,
	}

	if config.Producer.Debug != "" {
		_ = configMap.SetKey("debug", config.Producer.Debug)
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, err
	}

	return &Producer[T]{
		producer:     producer,
		config:       config,
		Logger:       logger,
		serializable: serializable,
	}, nil
}

// SendMessage - отправка сообщения в указанный топик
func (p *Producer[T]) SendMessage(topic string, msg *T, key []byte) (err error) {

	messageForProducer, err := p.serializable.Serialize(topic, msg)

	if err != nil {
		return err
	}

	deliveryChan := make(chan kafka.Event, 1)

	// Асинхронная отправка
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          messageForProducer,
		Key:            key,
	}, deliveryChan)

	if err != nil {
		return err
	}

	// Ждём подтверждения отправки сообщения
	eventFromProducer := <-deliveryChan
	m := eventFromProducer.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		err = fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	return err
}

// Close - закрытие продюсера по необходимости для экономии ресурсов
func (p *Producer[T]) Close() {
	// Ждём доставки всех сообщений перед закрытием в течение миллисекунд: FlushTimeoutMs
	p.producer.Flush(p.config.Producer.FlushTimeoutMs)
	p.producer.Close()
}
