package consumer

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/RAdevelop/ya_practicum-kafka/unit_1/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_1/go-app/internal/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer[T any] struct {
	consumer    *kafka.Consumer
	config      config.Config
	logger      *logger.Logger
	deserialize func([]byte, any) error
	batchSize   int
	batch       []*T
}

// NewConsumer - конструтор для консьюмера
func NewConsumer[T any](config config.Config, logger *logger.Logger, deserialize func([]byte, any) error, groupID string, batchSize int) (*Consumer[T], error) {
	if groupID == "" {
		return nil, fmt.Errorf("invalid groupID (\"%s\"), must be not empty string", groupID)
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("invalid batch size (%d), must be > 0", batchSize)
	}
	/*
		TODO добавить проверку batchSize <= N, где N некое максимальное число, чтобы при обработке сообщений не выделять слишком много памяти
			Вероятно, имеет смысла такой максимум установить в конфиге, и сравнивать с ним
	*/
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.Consumer.BootstrapServers,
		"group.id":          groupID,
		/*
			с какого места консьюмер начнет читать сообщения в партиции, если у него нет закоммиченного смещения (offset)
			- earliest:
				- Консьюмер начинает читать с самого первого доступного сообщения в партиции.
				- При последующих чтениях - будут получены новые сообщения.
			- latest:
				- Консьюмер начинает читать только новые сообщения, которые будут отправлены в топик после его запуска.
		*/
		"auto.offset.reset":        config.Consumer.AutoOffsetReset,
		"enable.auto.commit":       config.Consumer.EnableAutoCommit, // Включает/выключает фоновую автоматическую фиксацию (commit) смещений в брокере
		"enable.auto.offset.store": config.Consumer.EnableAutoCommit, // Включает/выключает автоматическое сохранение смещения в локальной памяти клиента
		"fetch.min.bytes":          config.Consumer.FetchMinBytes,    // Минимум 1 KB за один запрос
		"fetch.wait.max.ms":        config.Consumer.FetchWaitMaxMs,   // Ждём получение сообщения до FetchMaxWaitMs мс
	})
	if err != nil {
		return nil, err
	}

	return &Consumer[T]{
		consumer:    consumer,
		config:      config,
		logger:      logger,
		deserialize: deserialize,
		batch:       make([]*T, 0, batchSize),
		batchSize:   batchSize,
	}, nil
}

// SubscribeTopic - подписываемся на указанный топик
func (c *Consumer[T]) SubscribeTopic(topic string) error {
	err := c.consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return err
	}
	return nil
}

// Close - закрываем консьюмера по мере необходимости (для экономии ресурсов)
func (c *Consumer[T]) Close() error {
	if c.consumer != nil && c.consumer.IsClosed() {
		return nil
	}
	return c.consumer.Close()
}

/*
Consume - считываем сообщения из Кафки

ctx - для возможности отмены выполнения
processBatchCb - обработка сообщений по мере их чтения
*/
func (c *Consumer[T]) Consume(ctx context.Context, processBatchCb func(context.Context, *logger.Logger, []*T) error) {
	baseSleepInterval := 1_000 * time.Millisecond
	maxSleepInterval := baseSleepInterval * 10
	sleepInterval := baseSleepInterval
	sleepDuration := 0 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("The context canceled the execution for Consumer")
			return
		default:
			// Вычитываем сообщения в пачку
			var err error
			for len(c.batch) < c.batchSize {

				event := c.consumer.Poll(100)
				// Если нет события, мы немедленно возвращаемся в начало цикла и проверяем ctx
				if event == nil {
					c.logger.Info("There is no message to read")
					/*
						Возможность отложеннго повтора забрать сообщения (retry & backoff-тактика).
						Чтобы не пробовать в холостую забирать сообщения
						Например:
						- Если сообщений в принципе пока больше нет, то засыпаем на N сек/мс/наносек ...
						- С каждой такой итерацией такой "счетчик" увеличивался бы по экспоненте (* 2, * 4, * 8, ...)
						- Когда сообщения появятся, сбросить этот счетки
						- break - чтобы внешний цикл for повторился
					*/
					if sleepDuration > 0 {
						time.Sleep(sleepDuration)
						sleepInterval *= 2
						if sleepInterval > maxSleepInterval {
							sleepInterval = maxSleepInterval
						}
					}

					// Добавляем случайность ±20%
					jitter := time.Duration(rand.Float64() * float64(sleepInterval) * 0.2)
					sleepDuration = sleepInterval + jitter

					c.logger.Info("Sleeping for %v", sleepDuration)
					break
				}

				switch readingEvent := event.(type) {
				case *kafka.Message:

					sleepDuration = 0
					sleepInterval = baseSleepInterval

					// Если есть событие с сообщением, десериализуем его:
					var message *T
					err = c.deserialize(readingEvent.Value, &message)
					if err != nil {
						c.logger.Error("Consumer's deserialize error: %v", err)
						// положить такие сообщения в DLQ топик
						// идем за следующим сообщением:
						continue
					}

					c.batch = append(c.batch, message)

					logMsg := fmt.Sprintf("The consumer added data to the batch: %+v \n(Partition %d, Offset %d)\n\n", message, readingEvent.TopicPartition.Partition, readingEvent.TopicPartition.Offset)
					c.logger.Info(logMsg)

					// заголовки сообщения
					if readingEvent.Headers != nil {
						c.logger.Info("Headers: %v\n", readingEvent.Headers)
					}
				case kafka.Error:
					c.logger.Error("readingEvent.Code() = %v: readingEvent = %v, readingEvent.IsRetriable() = %v\n", readingEvent.Code(), readingEvent, readingEvent.IsRetriable())
				default:
					c.logger.Info("Ignored: %v\n", readingEvent)
				}
			}

			// Если пачка набрана — обрабатываем и коммитим смещение
			if len(c.batch) > 0 {

				if processBatchCb != nil {
					err = processBatchCb(ctx, c.logger, c.batch)
					if err != nil {
						c.logger.Error("processBatchCb error: %v", err)
						/*
							В задаче не было такого требования по обработки подобных ситуаций. Опишу текстом:
							- Можно добавить стратегию повторных попыток выполнения processBatchCb с backoff-тактикой.
								- Если после попыток все равно есть ошибка, положить такие сообщения из c.batch в DLQ топик
						*/
					}
				}

				// Коммитим оффсет всей пачки
				// TODO только если успешно выполнили processBatchCb(...)
				_, err = c.consumer.Commit()
				if err != nil {
					c.logger.Error("commit offset error: %v", err)
				}

				// Очищаем пачку
				c.batch = c.batch[:0]
			}
		}
	}
}
