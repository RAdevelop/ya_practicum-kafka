package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/RAdevelop/ya_practicum-kafka/unit_5/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_5/go-app/internal/consumer"
	"github.com/RAdevelop/ya_practicum-kafka/unit_5/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_5/go-app/internal/models"
)

func main() {

	var cfg config.Config
	cfg.Load(".env")

//TODO del
	log.Printf("%+v", cfg.Consumer)
	log.Printf("%+v", cfg.Producer)
	log.Printf("%+v", cfg.Topic)
	/*
	   TODO DEL
	   select {}

	   	return
	*/

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	logThis := logger.New("InMainApp")

	// создаем консьюмера для чтения из topic-1
	subscriberTopic1, loggerTopic1, deferCloseFuncSubscriberTopic1, err := consumerCreate("ConsumerTopic1", cfg, cfg.Consumer.GroupIdTopic1, 1)
	if err != nil {
		loggerTopic1.Error("Error on Consumer initialization: %v", err)
		return
	}
	defer deferCloseFuncSubscriberTopic1()

	// подключаемся к топику
	err = subscriberTopic1.SubscribeTopic(cfg.Topic.Topic1)
	if err != nil {
		loggerTopic1.Error("Error on subscribe to a topic: %v", err)
	}
	loggerTopic1.Info("Subscribed to a topic: %s", cfg.Topic.Topic1)

	//TODO del
	//return

	// создаем консьюмера для чтения из topic-2
	subscriberTopic2, loggerTopic2, deferCloseFuncSubscriberTopic2, err := consumerCreate("ConsumerTopic2", cfg, cfg.Consumer.GroupIdTopic2, 1)
	if err != nil {
		loggerTopic2.Error("Error on initialization: %v", err)
		return
	}
	defer deferCloseFuncSubscriberTopic2()

	// подключаемся к топику topic-2
	err = subscriberTopic2.SubscribeTopic(cfg.Topic.Topic2)
	if err != nil {
		loggerTopic2.Error("Error on subscribe to a topic: %v", err)
	}
	loggerTopic2.Info("Subscribed to a topic: %s", cfg.Topic.Topic2)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		subscriberTopic1.Consume(ctx, processMessage)
	}()
	/*
		//TODO
		wg.Add(1)
		go func() {
			defer wg.Done()
			subscriberTopic2.Consume(ctx, processMessage)//должны быть ошибки доступа
		}()
	*/
	//Обработка прерывания работы приложения, например, по CTR + c:
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logThis.Info("Interrupt signal received")
	ctxCancel()
	wg.Wait()
	logThis.Info("App is closed")
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

// processMessage - callback функция для обработки сообщений в процессе их получения из Кафки
func processMessage(ctx context.Context, logger *logger.Logger, messages []*models.Message) error {
	/*
		обработка сообщений, полученных из Kafka, например:
		   - сохранение данных в БД
		   - отправка в какой-нибудь сервис
		   - и тп
	*/

	//пока просто выведем сообщения:
	for _, msg := range messages {
		data, _ := json.MarshalIndent(msg, "", "  ")
		logger.Info("Message:\n%s", string(data))
	}

	return nil
}
