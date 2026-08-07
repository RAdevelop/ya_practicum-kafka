package main

import (
    "context"
    "encoding/json"
    "os"
    "os/signal"
    "sync"
    "syscall"

    "github.com/RAdevelop/ya_practicum-kafka/unit_4/go-app/internal/config"
    "github.com/RAdevelop/ya_practicum-kafka/unit_4/go-app/internal/consumer"
    "github.com/RAdevelop/ya_practicum-kafka/unit_4/go-app/internal/logger"
    "github.com/RAdevelop/ya_practicum-kafka/unit_4/go-app/internal/models"
)

const (
    topicUsers = "pg.public.users"  // TODO hardcode - вынести в конфиг?
    topicOrdes = "pg.public.orders" // TODO hardcode - вынести в конфиг?
)

func main() {

    ctx, ctxCancel := context.WithCancel(context.Background())
    defer ctxCancel()

    logThis := logger.New("InMainApp")

    var cfg config.Config
    cfg.Load(".env")

    var wg sync.WaitGroup

    // создаем консьюмера для чтения сообщения по 10 шт.
    subscriberUsers, loggerUsers, deferCloseFuncSubscriberUsers, err := consumerCreate("ConsumerUsers", cfg, cfg.Consumer.GroupIdUsers, 1)
    if err != nil {
        loggerUsers.Error("Error on Consumer initialization: %v", err)
        return
    }
    defer deferCloseFuncSubscriberUsers()

    // подключаемся к топику
    err = subscriberUsers.SubscribeTopic(topicUsers)
    if err != nil {
        loggerUsers.Error("Error on subscribe to a topic: %v", err)
    }
    loggerUsers.Info("Subscribed to a topic: %s", topicUsers)

    // создаем консьюмера для чтения сообщения по одной шт
    subscriberOrders, loggerOrders, deferCloseFuncSubscriberOrders, err := consumerCreate("ConsumerOrders", cfg, cfg.Consumer.GroupIdOrders, 1)
    if err != nil {
        loggerOrders.Error("Error on initialization: %v", err)
        return
    }
    defer deferCloseFuncSubscriberOrders()

    // подключаемся к топику
    err = subscriberOrders.SubscribeTopic(topicOrdes)
    if err != nil {
        loggerOrders.Error("Error on subscribe to a topic: %v", err)
    }
    loggerOrders.Info("Subscribed to a topic: %s", topicOrdes)
    wg.Add(1)
    go func() {
        defer wg.Done()
        subscriberOrders.Consume(ctx, processMessage)
    }()

    wg.Add(1)
    go func() {
        defer wg.Done()
        subscriberUsers.Consume(ctx, processMessage)
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
