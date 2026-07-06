package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/RAdevelop/ya_practicum-kafka/unit_1/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_1/go-app/internal/consumer"
	"github.com/RAdevelop/ya_practicum-kafka/unit_1/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_1/go-app/internal/models"
	"github.com/RAdevelop/ya_practicum-kafka/unit_1/go-app/internal/producer"
)

const (
	topic       = "topic_unit_1" // TODO hardcode - вынести в конфиг?
	singleGroup = "single-group" // TODO hardcode - вынести в конфиг?
	batchGroup  = "batch-group"  // TODO hardcode - вынести в конфиг?
)

func main() {

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	logThis := logger.New("InMainApp")

	var cfg config.Config
	cfg.Load(".env")
	var wg sync.WaitGroup

	// создаем продюсера
	logProducer := logger.New("Producer")
	publisher, err := producer.NewProducer[models.Message](cfg, logProducer, json.Marshal)
	if err != nil {
		logProducer.Error("Error connecting the producer: %v", err)
		return
	}
	defer publisher.Close()
	logProducer.Info("Producer has been connected to the brokers")

	countMsg := 100
	// Канал передачи сообщений между генератором и отправщиком
	produceChannel := make(chan *models.Message, countMsg)

	// генерация сообщений:
	go generateMessage(produceChannel, countMsg)

	wg.Add(3)

	// отправка сообщений:
	go func() {
		defer wg.Done()
		produceMessage(topic, publisher, logProducer, produceChannel)
	}()

	// создаем консьюмера для чтения сообщения по 10  шт
	subscriberButchGroup, loggerButchGroup, deferCloseFuncSubscriberButchGroup, err := consumerCreate("ConsumerButchGroup", cfg, batchGroup, 10)
	if err != nil {
		loggerButchGroup.Error("Error on Consumer initialization: %v", err)
		return
	}
	defer deferCloseFuncSubscriberButchGroup()

	// подключаемся к топику
	err = subscriberButchGroup.SubscribeTopic(topic)
	if err != nil {
		loggerButchGroup.Error("Error on subscribe to a topic: %v", err)
	}
	loggerButchGroup.Info("Subscribed to a topic: %s", topic)

	// создаем консьюмера для чтения сообщения по одной шт
	subscriberSingleGroup, loggerSingleGroup, deferCloseFuncSubscriberSingleGroup, err := consumerCreate("ConsumerSingleGroup", cfg, singleGroup, 1)
	if err != nil {
		loggerSingleGroup.Error("Error on initialization: %v", err)
		return
	}
	defer deferCloseFuncSubscriberSingleGroup()

	// подключаемся к топику
	err = subscriberSingleGroup.SubscribeTopic(topic)
	if err != nil {
		loggerSingleGroup.Error("Error on subscribe to a topic: %v", err)
	}
	loggerSingleGroup.Info("Subscribed to a topic: %s", topic)

	go func() {
		defer wg.Done()
		subscriberSingleGroup.Consume(ctx, processBatchCbSingleGroup)
	}()

	go func() {
		defer wg.Done()
		subscriberButchGroup.Consume(ctx, processBatchCbButchGroup)
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

// produceMessage - отпрака сообщений в Кафка
func produceMessage(topic string, publisher *producer.Producer[models.Message], logger *logger.Logger, produceChannel <-chan *models.Message) {
	for message := range produceChannel {
		err := publisher.SendMessage(topic, message)
		if err != nil {
			logger.Error("Error sending the message (%v):\n%v", err, message)
		} else {
			logger.Info("Message has been sent:\n%v", message)
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

// processBatchCbSingleGroup - callback функция для обработки сообщений в процессе их получения из Кафки
func processBatchCbSingleGroup(ctx context.Context, logger *logger.Logger, messages []*models.Message) error {
	/*
		обработка сообщений, полученных из Кафка, например:
		- сохранние данных в БД
		- отправка в какой-нибудь сервисы
		- и тп
	*/
	//пока просто выведем сообщения:
	logger.Info("Processing batch:\n%v\n\n", messages)

	return nil
}

// processBatchCbButchGroup - callback функция для обработки сообщений в процессе их получения из Кафки
func processBatchCbButchGroup(ctx context.Context, logger *logger.Logger, messages []*models.Message) error {
	/*
		обработка сообщений, полученных из Кафка, например:
		- сохранние данных в БД
		- отправка в какой-нибудь сервисы
		- и тп
	*/
	//пока просто выведем сообщения:
	logger.Info("Processing batch:\n\n%v\n\n", messages)

	return nil
}
