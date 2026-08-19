package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/RAdevelop/ya_practicum-kafka/unit_5/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_5/go-app/internal/consumer"
	"github.com/RAdevelop/ya_practicum-kafka/unit_5/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_5/go-app/internal/models"
	"github.com/RAdevelop/ya_practicum-kafka/unit_5/go-app/internal/producer"
)

func main() {

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	logThis := logger.New("InMainApp")

	var cfg config.Config
	cfg.Load(".env")

	// создаем продюсера
	logProducer := logger.New("Producer")
	publisher, err := producer.NewProducer[models.Message](cfg, logProducer, json.Marshal)
	if err != nil {
		logProducer.Error("Error connecting the producer: %v", err)
		return
	}
	defer publisher.Close()
	logProducer.Info("Producer has been connected to the brokers")

	countMsg := 2
	// Канал передачи сообщений между генератором и отправщиком
	produceChannel := make(chan *models.Message, countMsg)

	// генерация сообщений:
	go generateMessage(produceChannel, countMsg)

	var wg sync.WaitGroup
	wg.Add(1)

	// отправка сообщений:
	go func() {
		defer wg.Done()
		produceMessage(cfg, publisher, logProducer, produceChannel)
	}()

	// создаем консьюмера для чтения сообщения по 10 шт.
	subscriberTopic1, loggerSubscriberTopic1, deferCloseFuncSubscriberTopic1, err := consumerCreate("ConsumerTopic1", cfg, cfg.Consumer.GroupIdTopic1, 10)
	if err != nil {
		loggerSubscriberTopic1.Error("Error on Consumer initialization: %v", err)
		return
	}
	defer deferCloseFuncSubscriberTopic1()

	// подключаемся к топику
	err = subscriberTopic1.SubscribeTopic(cfg.Topic.Topic1)
	if err != nil {
		loggerSubscriberTopic1.Error("Error on subscribe to a topic: %v", err)
	}
	loggerSubscriberTopic1.Info("Subscribed to a topic: %s", cfg.Topic.Topic1)

	// создаем консьюмера для чтения сообщения по одной шт
	subscriberTopic2, loggerSubscriberTopic2, deferCloseFuncSubscriberTopic2, err := consumerCreate("ConsumerTopic2", cfg, cfg.Consumer.GroupIdTopic2, 1)
	if err != nil {
		loggerSubscriberTopic2.Error("Error on initialization: %v", err)
		return
	}
	defer deferCloseFuncSubscriberTopic2()

	// подключаемся к топику
	err = subscriberTopic2.SubscribeTopic(cfg.Topic.Topic2)
	if err != nil {
		loggerSubscriberTopic2.Error("Error on subscribe to a topic: %v", err)
	}
	loggerSubscriberTopic2.Info("Subscribed to a topic: %s", cfg.Topic.Topic2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		subscriberTopic2.Consume(ctx, processBatchCb) //будут ошибки для topic-2, так как консьюмерам не дали доступ к этому топику
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		subscriberTopic1.Consume(ctx, processBatchCb)
	}()

	//Обработка прерывания работы приложения, например, по CTR + c:
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logThis.Info("Interrupt signal received")
	ctxCancel()
	wg.Wait()
	logThis.Info("App is closed")
}

// produceMessage - отправка сообщений в Кафка
func produceMessage(config config.Config, publisher *producer.Producer[models.Message], logger *logger.Logger, produceChannel <-chan *models.Message) {

	for message := range produceChannel {

		errTopic1 := publisher.SendMessage(config.Topic.Topic1, message)
		if errTopic1 != nil {
			logger.Error("Error sending the message to topic: %s (%v):\n%v", config.Topic.Topic1, errTopic1, message)
		} else {
			logger.Info("Message has been sent to topic: %s :\n%v", config.Topic.Topic1, message)
		}

		errTopic2 := publisher.SendMessage(config.Topic.Topic2, message)
		if errTopic2 != nil {
			logger.Error("Error sending the message to topic: %s (%v):\n%v", config.Topic.Topic2, errTopic2, message)
		} else {
			logger.Info("Message has been sent to topic: %s :\n%v", config.Topic.Topic2, message)
		}

	}
}

// generateMessage - генерируем сообщения в количестве countMsg
func generateMessage(produceChannel chan<- *models.Message, countMsg int) {
	defer close(produceChannel)
	/*
	   Для ID сообщений лучше использовать UUID. Тогда при работе N шт продюсеров, и M штук консьюмеров они не будут:
	   - генерировать (отправлять в кафку) одни и те же сообщения
	   - читать из кафки одни и те же сообщения
	*/
	for i := 0; i < countMsg; i++ {
		msg := &models.Message{
			ID:      i,
			Payload: "Hello from producer",
			Ts:      time.Now().UnixNano(),
		}
		produceChannel <- msg
	}
}

func consumerCreate[T models.Message](loggerPrefix string, config config.Config, groupID string, batchSize int) (subscriber *consumer.Consumer[T], logMe *logger.Logger, deferCloseFunc func(), err error) {

	logMe = logger.New(loggerPrefix)
	subscriber, err = consumer.NewConsumer[T](config, logMe, json.Unmarshal, groupID, batchSize)

	if err != nil {
		return nil, logMe, nil, err
	}

	deferCloseFunc = func() {
		err = subscriber.Close()
		if err != nil {
			logMe.Error("Error on close: %v", err)
		}
	}

	return subscriber, logMe, deferCloseFunc, nil
}

// processBatchCb - callback функция для обработки сообщений в процессе их получения из Кафки
func processBatchCb(ctx context.Context, logger *logger.Logger, messages []*models.Message) error {
	/*
		обработка сообщений, полученных из Кафка, например:
		- сохранние данных в БД
		- отправка в какой-нибудь сервисы
		- и тп
	*/
	//пока просто выведем сообщения:
	logger.Info("Processing batch:\n%v\n", messages)

	return nil
}
