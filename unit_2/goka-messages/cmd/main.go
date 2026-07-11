package main

import (
	"context"
	"fmt"
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
	/*
		fmt.Printf("%+v\n", cfg.Emitter.Brokers)
		fmt.Printf("%+v\n", cfg.Processor.Brokers)
	*/

	ctx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	var wg sync.WaitGroup

	// 1. запускаем эмиттер для загрузки запрещенных слов
	wg.Add(1)
	go func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()

		bwLogger := logger.New("[BadWordEmitter]")
		bwEmitter, err := emitter.NewEmitter(cfg.Topic.BadWords, cfg, new(codec.String))
		if err != nil {
			bwLogger.Error("Failed to create BadWord Emitter %v", err)
			return
		}
		defer func(bwEmitter *emitter.Emitter) {
			err = bwEmitter.Finish()
			if err != nil {
				bwLogger.Error("Failed to finish BadWord Emitter %v", err)
			}
		}(bwEmitter)

		badWordList := []string{"bad", "word"}

		for _, badWord := range badWordList {
			err = bwEmitter.EmitSync("bad_word", badWord)
			if err != nil {
				bwLogger.Error("Failed to emit BadWord Emitter %v", err)
			} else {
				bwLogger.Info("Emitted BadWord %s", badWord)
			}
		}
	}(ctx, &wg)
	// 2. запускаем эмиттер для генерации сообщений
	wg.Add(1)
	go func(ctx context.Context, wg *sync.WaitGroup) {
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
	}(ctx, &wg)
	// 3. запускаем эмиттер для загрузки обновления кто кого заблокировал
	// 4. запускаем процессор для проверки возможности отправки отдельно взятого сообщения между пользователями
	// 5. запускаем процессор для выполнения задачи цензуры над сообщением
	wg.Add(1)
	go func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()

		censor := processor.NewCensor(cfg)
		censor.Cens(ctx)

	}(ctx, &wg)

	// 6. запускаем процессор для вывода на экран, какое кому сообщение "будет" отправлено
	wg.Add(1)
	go func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		var messageSender = processor.NewMessageSender()
		messageSender.Send(ctx, cfg, new(jsCode.JsonCodec[model.Message]))
	}(ctx, &wg)

	go func() {
		defer func(pid int, signum syscall.Signal) {
			err := syscall.Kill(pid, signum)
			if err != nil {
				fmt.Printf("Failed to kill process %d: %v", pid, err)
				return
			}
			fmt.Printf("App has been stopped (pid: %d)", pid)
		}(os.Getpid(), syscall.SIGTERM)
		wg.Wait()
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait
	cancelApp()

}

var user1 = model.User{
	ID:            1,
	AcceptBadWord: true,
}
var user2 = model.User{
	ID:            2,
	AcceptBadWord: true,
}
var user3 = model.User{
	ID:            3,
	AcceptBadWord: true,
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
