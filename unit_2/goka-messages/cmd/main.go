package main

import (
	"context"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	jsCode "github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/emitter"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/processor"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/lovoo/goka/codec"
)

func main() {

	var cfg config.Config
	cfg.Load(".env")

	ctx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	var wg sync.WaitGroup

	// запускаем процессор для обновления состояния по блокировкам пользователей
	//wg.Add(1)
	//go processorBlockUsers(ctx, cfg, &wg)

	// запускаем процессор для выполнения задачи цензуры над сообщением
	wg.Add(1)
	go processorCensor(ctx, cfg, &wg)

	// запускаем процессор для вывода на экран, какое кому сообщение "будет" отправлено
	wg.Add(1)
	go processorMessageSender(ctx, cfg, &wg)

	// запускаем эмиттер для событий кто кого заблокировал
	//wg.Add(1)
	//go emitterBlockedUsers(ctx, cfg, &wg)

	// запускаем эмиттер для загрузки запрещенных слов
	wg.Add(1)
	go emitterBadWord(ctx, cfg, &wg)

	// запускаем эмиттер для генерации сообщений
	wg.Add(1)
	go emitterMessage(ctx, cfg, &wg)

	// Обрабатываем сигнал в отдельной горутине
	go func() {
		wait := make(chan os.Signal, 1)
		signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
		<-wait
		log.Println("Received shutdown signal, cancelling context...")
		cancelApp()
	}()

	// Ждем завершения всех горутин
	wg.Wait()
	log.Println("All goroutines finished")
}

func emitterBlockedUsers(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()

	buLogger := logger.New("[BlockedUsersEmitter]")
	buEmitter, err := emitter.NewEmitter(cfg.Topic.BlockedUsers, cfg, new(codec.String))
	if err != nil {
		buLogger.Error("Failed to create BadWord Emitter %v", err)
		return
	}

	ticker := time.NewTicker(3 * time.Second)

	defer func(buEmitter *emitter.Emitter) {
		ticker.Stop()
		err = buEmitter.Finish()
		if err != nil {
			buLogger.Error("Failed to finish BlockedUsers Emitter %v", err)
			return
		}
		buLogger.Info("BlockedUsers Emitter stopped")
	}(buEmitter)

	blockEvents := blockEventList()

	for {
		select {
		case <-ctx.Done():
			return

		// эмуляция блокировок/разблокировок между пользователями
		case <-ticker.C:

			indexEvent := rand.IntN(len(blockEvents))

			blockEvent := blockEvents[indexEvent].Event
			blockerID := blockEvents[indexEvent].BlockerID

			err = buEmitter.EmitSync(blockerID, blockEvent)
			if err != nil {
				buLogger.Error("Failed to emit blockEvent Emitter %v", err)
			} else {
				buLogger.Info("Emitted blockEvent %s", blockEvent)
			}
		}
	}
}

func emitterBadWord(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()

	bwLogger := logger.New("[BadWordEmitter]")
	bwEmitter, err := emitter.NewEmitter(cfg.Topic.BadWords, cfg, new(codec.String))
	if err != nil {
		bwLogger.Error("Failed to create BadWord Emitter %v", err)
		return
	}

	ticker := time.NewTicker(3 * time.Second)

	defer func(bwEmitter *emitter.Emitter) {
		ticker.Stop()
		err = bwEmitter.Finish()
		if err != nil {
			bwLogger.Error("Failed to finish BadWord Emitter %v", err)
		}
	}(bwEmitter)

	badWordList := []string{"bad", "word", "world"}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			indexWord := rand.IntN(len(badWordList))
			badWord := badWordList[indexWord]
			err = bwEmitter.EmitSync(cfg.KeyTopic.BadWords, badWord)
			if err != nil {
				bwLogger.Error("Failed to emit BadWord Emitter %v", err)
			} else {
				bwLogger.Info("Emitted BadWord %s", badWord)
			}
		}
	}
}

func emitterMessage(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()
	msgLogger := logger.New("[MessageEmitter]")
	msgEmitter, err := emitter.NewEmitter(cfg.Topic.Messages, cfg, new(jsCode.JsonCodec[model.Message]))
	if err != nil {
		msgLogger.Error("Failed to create MessageEmitter %v", err)
		return
	}
	defer func(msgEmitter *emitter.Emitter) {
		err = msgEmitter.Finish()
		if err != nil {
			msgLogger.Error("Failed to finish MessageEmitter %v", err)
		}
	}(msgEmitter)

	var msgIndex = 0
	for {
		select {
		case <-ctx.Done():
			msgLogger.Info("emitter for message generate has been stopped")
			return
		case <-time.After(time.Second):

			msgLogger.Info("msgIndex = %d", msgIndex)
			if msgIndex >= len(messages) {
				return
			}
			var msg model.Message
			msg = messages[msgIndex]
			err = msgEmitter.EmitSync(msg.IDToString(), msg)
			if err != nil {
				msgLogger.Error("Failed to emit message: %#v, error %v", msg, err)
				break
			}
			msgLogger.Info("Emitted Message: %#v", msg)
			msgIndex++
		}
	}
}

func processorBlockUsers(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()

	pBlockUsers := processor.NewUserBlocker(cfg)
	pBlockUsers.Run(ctx)
}

func processorMessageSender(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()
	var messageSender = processor.NewMessageSender()
	messageSender.Send(ctx, cfg, new(jsCode.JsonCodec[model.Message]))
}

func processorCensor(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()

	censor := processor.NewCensor(cfg)
	censor.Run(ctx)
}

var user1 = model.User{
	ID: 1,
}
var user2 = model.User{
	ID: 2,
}
var user3 = model.User{
	ID: 3,
}

var messages = []model.Message{
	{
		ID:         1,
		FromUserID: user1.ID,
		ToUserID:   user2.ID,
		Text:       "hello world",
	},
	{
		ID:         2,
		FromUserID: user1.ID,
		ToUserID:   user3.ID,
		Text:       "hello world",
	},
	{
		ID:         3,
		FromUserID: user2.ID,
		ToUserID:   user1.ID,
		Text:       "hello world",
	},
	{
		ID:         4,
		FromUserID: user2.ID,
		ToUserID:   user3.ID,
		Text:       "hello world",
	},
	{
		ID:         5,
		FromUserID: user3.ID,
		ToUserID:   user1.ID,
		Text:       "hello world",
	},
	{
		ID:         6,
		FromUserID: user3.ID,
		ToUserID:   user2.ID,
		Text:       "hello world",
	},
}

type BlockEvent struct {
	Event     string
	BlockerID string
}

func blockEventList() []BlockEvent {
	return []BlockEvent{
		{Event: "block:1:2", BlockerID: "1"}, // пользователь 1 блокирует пользователя 2
		{Event: "block:1:3", BlockerID: "1"}, // пользователь 1 блокирует пользователя 3
		{Event: "block:2:1", BlockerID: "2"}, // пользователь 2 блокирует пользователя 1
		{Event: "block:2:3", BlockerID: "2"}, // пользователь 2 блокирует пользователя 3
		{Event: "block:3:1", BlockerID: "3"}, // пользователь 3 блокирует пользователя 1
		{Event: "block:3:2", BlockerID: "3"}, // пользователь 3 блокирует пользователя 2

		{Event: "unblock:1:2", BlockerID: "1"}, // пользователь 1 разблокирует пользователя 2
		{Event: "unblock:1:3", BlockerID: "1"}, // пользователь 1 разблокирует пользователя 3
		{Event: "unblock:2:1", BlockerID: "2"}, // пользователь 2 разблокирует пользователя 1
		{Event: "unblock:2:3", BlockerID: "2"}, // пользователь 2 разблокирует пользователя 3
		{Event: "unblock:3:1", BlockerID: "3"}, // пользователь 3 разблокирует пользователя 1
		{Event: "unblock:3:2", BlockerID: "3"}, // пользователь 3 разблокирует пользователя 2
	}
}
