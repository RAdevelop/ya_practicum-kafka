package producer

import (
	"fmt"

	"github.com/RAdevelop/ya_practicum-kafka/unit_1/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_1/go-app/internal/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer - продюсер для отправки сообщений в Kafka
type Producer[T any] struct {
	producer  *kafka.Producer
	config    config.Config
	logger    *logger.Logger
	serialize func(T any) ([]byte, error)
}

// NewProducer - конструтор для продюсера
func NewProducer[T any](config config.Config, logger *logger.Logger, serializer func(T any) ([]byte, error)) (*Producer[T], error) {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.Producer.BootstrapServers,
		// Гарантия At Least Once
		"acks": config.Producer.Acks, // Подтверждение от всех реплик
		// Количество повторных попыток, которые продюсер сделает, чтобы отправить сообщение, если при первой попытке произошла временная ошибка:
		"retries":            config.Producer.Retries,
		"retry.backoff.ms":   config.Producer.RetryBackoffMs,    // Пауза между попытками
		"enable.idempotence": config.Producer.EnableIdempotence, // Идемпотентность (защита от дублей)
		//Определяет, сколько времени клиент (продюсер или консьюмер) будет ждать установки TCP-соединения с брокером:
		"socket.connection.setup.timeout.ms": config.Producer.SocketConnectionSetupTimeoutMs,
		// Определяет максимальное время ожидания ответа на уже отправленный запрос по уже установленному соединению:
		"socket.timeout.ms": config.Producer.SocketTimeoutMs,
	})
	if err != nil {
		return nil, err
	}

	return &Producer[T]{
		producer:  producer,
		config:    config,
		logger:    logger,
		serialize: serializer,
	}, nil
}

// SendMessage - отправка сообщения в указаный топик
func (p *Producer[T]) SendMessage(topic string, msg *T) (err error) {

	messageForProducer, err := p.serialize(msg)
	if err != nil {
		return err
	}
	p.logger.Info("The producer created a serialized message: %v", messageForProducer)

	deliveryChan := make(chan kafka.Event, 1)

	// Асинхронная отправка
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          messageForProducer,
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
	// Ждём доставки всех сообщений перед закрытием в течение милисекунд: FlushTimeoutMs
	p.producer.Flush(p.config.Producer.FlushTimeoutMs)
	p.producer.Close()
}
