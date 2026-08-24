package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/consumer"
	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/models"
	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/producer"
	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/serializer"
)

func main() {

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	logApp := logger.New("InMainApp")

	var cfg config.Config
	cfg.Load(".env")

	serialization, err := serializer.NewJson[models.Message](cfg)
	if err != nil {
		logApp.Error("error serializer create: %v", err)
		return
	}
	defer func() {
		err := serialization.Close()
		if err != nil {
			logApp.Error("error serializer close: %v", err)
		}
	}()

	// создаем продюсера
	publisher, err := producer.NewProducer[models.Message](cfg, logger.New("Producer"), serialization)
	if err != nil {
		logApp.Error("Error connecting the producer: %v", err)
		return
	}
	defer publisher.Close()
	logApp.Info("Producer has been connected to the brokers")

	countMsg := 2
	// Канал передачи сообщений между генератором и отправителем
	produceChannel := make(chan *models.Message, countMsg)

	// генерация сообщений:
	go generateMessage(produceChannel, countMsg)

	var wg sync.WaitGroup
	wg.Add(1)

	// отправка сообщений:
	go func(ctx context.Context) {
		defer wg.Done()
		produceMessage(cfg, publisher, produceChannel)
	}(ctx)

	// создаем консьюмера для чтения сообщения по одной шт
	subscriber, deferCloseFuncSubscriber, err := consumerCreate("Consumer", cfg, serialization, cfg.Consumer.GroupId, 10)
	if err != nil {
		logApp.Error("Error on initialization: %v", err)
		return
	}
	defer deferCloseFuncSubscriber()

	// подключаемся к "топику"
	err = subscriber.SubscribeTopic(cfg.Topic.Metric)
	if err != nil {
		subscriber.Logger.Error("Error on subscribe to a topic: %v", err)
	}
	subscriber.Logger.Info("Subscribed to a topic: %s", cfg.Topic.Metric)

	wg.Add(1)
	go func() {
		defer wg.Done()
		subscriber.Consume(ctx, processBatchCb)
	}()

	//Обработка прерывания работы приложения, например, по CTR + c:
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logApp.Info("Interrupt signal received")
	ctxCancel()
	wg.Wait()
	logApp.Info("App is closed")
}

// produceMessage - отправка сообщений в Кафка
func produceMessage(config config.Config, publisher *producer.Producer[models.Message], produceChannel <-chan *models.Message) {

	for message := range produceChannel {

		errTopicMetric := publisher.SendMessage(config.Topic.Metric, message, message.IdAsByte())
		if errTopicMetric != nil {
			publisher.Logger.Error("Error sending the message to topic: %s (%v):\n%v", config.Topic.Metric, errTopicMetric, message)
		} else {
			publisher.Logger.Info("Message has been sent to topic: %s :\n%v", config.Topic.Metric, message)
		}
	}
}

// generateMessage - генерируем сообщения в количестве countMsg
func generateMessage(produceChannel chan<- *models.Message, countMsg int) {
	defer close(produceChannel)
	/*
		   Для ID сообщений лучше использовать UUID. Тогда при работе N шт. продюсеров, и M штук консьюмеров они не будут:
		- генерировать (отправлять в кафку) одни и те же сообщения
		- читать из кафки одни и те же сообщения
	*/
	for i := 1; i < countMsg+1; i++ {
		msg := &models.Message{
			ID:    int64(i),
			MType: "counter",
			Delta: new(int64(i)),
		}
		produceChannel <- msg
	}
}

func consumerCreate[T models.Message](loggerPrefix string, config config.Config, serializable serializer.Serializable[T], groupID string, batchSize int) (subscriber *consumer.Consumer[T], deferCloseFunc func(), err error) {

	subscriber, err = consumer.NewConsumer[T](config, logger.New(loggerPrefix), serializable, groupID, batchSize)

	if err != nil {
		return nil, nil, err
	}

	deferCloseFunc = func() {
		err = subscriber.Close()
		if err != nil {
			subscriber.Logger.Error("Error on close: %v", err)
		}
	}

	return subscriber, deferCloseFunc, nil
}

// processBatchCb - callback функция для обработки сообщений в процессе их получения из Кафки
func processBatchCb(ctx context.Context, logger *logger.Logger, messages []*models.Message) error {
	/*
		обработка сообщений, полученных из "Кафка", например:
		- сохранение данных в БД
		- отправка в какой-нибудь сервис
		- и тп
	*/
	//пока просто выведем сообщения:
	logger.Info("Processing batch:\n%v\n", messages)

	return nil
}
