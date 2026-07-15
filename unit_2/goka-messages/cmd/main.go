package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/api"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/emitter"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/processor"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/view"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/store"
	"github.com/lovoo/goka/codec"
)

func main() {

	appLogger := logger.New("[AppLogger]")
	var cfg config.Config
	cfg.Load(".env")
	ctx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	badWordsViewLogger := logger.New("[BadWordsView]")
	badWordsView, err := view.NewView(ctx, cfg.ViewTable.BadWords, new(jsCodec.JsonCodec[store.BadWordsStore]), cfg, badWordsViewLogger)
	if err != nil {
		badWordsViewLogger.Error("Failed to create view: %v", err)
		return
	}

	blockedUsersViewLogger := logger.New("[BlockedUsersView]")
	blockedUserView, err := view.NewView(ctx, cfg.ViewTable.BlockedUsers, new(jsCodec.JsonCodec[*store.BlockedUsersStore]), cfg, blockedUsersViewLogger)
	if err != nil {
		blockedUsersViewLogger.Error("Failed to create view: %v", err)
		return
	}

	badWordEmitter, err := emitter.NewEmitter(cfg.Topic.BadWords, cfg, new(codec.String))
	if err != nil {
		appLogger.Error("Failed to create badWordEmitter: %v", err)
		return
	}
	defer func() {
		err = badWordEmitter.Finish()
		if err != nil {
			appLogger.Error("Failed to finish BadWord Emitter %v", err)
		}
	}()
	blockUserEmitter, err := emitter.NewEmitter(cfg.Topic.BlockedUsers, cfg, new(codec.String))
	if err != nil {
		appLogger.Error("Failed to create BlockUser Emitter: %v", err)
		return
	}
	defer func() {
		err = blockUserEmitter.Finish()
		if err != nil {
			appLogger.Error("Failed to finish BlockUser Emitter %v", err)
		}
	}()
	messageEmitter, err := emitter.NewEmitter(cfg.Topic.Messages, cfg, new(jsCodec.JsonCodec[model.Message]))
	if err != nil {
		appLogger.Error("Failed to create Message Emitter: %v", err)
		return
	}
	defer func() {
		err = messageEmitter.Finish()
		if err != nil {
			appLogger.Error("Failed to finish Message Emitter %v", err)
		}
	}()

	emitters := &api.Emitters{
		BadWords:  badWordEmitter,
		Messages:  messageEmitter,
		BlockUser: blockUserEmitter,
	}

	views := &api.Views{
		BadWordsView:    badWordsView,
		BlockedUserView: blockedUserView,
	}

	handlers := api.NewHandlers(cfg, views, emitters)

	server := api.NewServer(handlers)

	var wg sync.WaitGroup

	wg.Add(1)
	go processorBadWord(ctx, cfg, &wg)

	wg.Add(1)
	go processorBlockUser(ctx, cfg, &wg)

	wg.Add(1)
	go processorCensor(views, ctx, cfg, &wg)

	wg.Add(1)
	go processorMessageSender(ctx, cfg, &wg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		if err != nil {
			return
		}
	}()

	go func() {
		wait := make(chan os.Signal, 1)
		signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
		<-wait
		log.Println("Received shutdown signal, cancelling context...")
		cancelApp()
	}()

	wg.Wait()
}

func processorBadWord(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()

	censor := processor.NewBadWord(cfg)
	censor.Run(ctx)
}

func processorBlockUser(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()

	pBlockUsers := processor.NewUserBlocker(cfg)
	pBlockUsers.Run(ctx)
}

func processorCensor(views *api.Views, ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()

	censor := processor.NewCensor(cfg, views)
	censor.Run(ctx)
}

func processorMessageSender(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()
	var messageSender = processor.NewMessageSender()
	messageSender.Run(ctx, cfg, new(jsCodec.JsonCodec[model.Message]))
}
