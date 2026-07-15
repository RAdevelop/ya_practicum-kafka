package processor

import (
	"context"
	"log"
	"strings"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/store"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

type BadWord struct {
	logger *logger.Logger
	config config.Config
}

func NewBadWord(config config.Config) *BadWord {
	return &BadWord{
		logger: logger.New("[BadWordProcessor]"),
		config: config,
	}
}

// Run - запуск процесса актуализации списка запрещенных слов
func (bw *BadWord) Run(ctx context.Context) {
	codecBabWord := new(jsCodec.JsonCodec[store.BadWordsStore])

	// определяем группу для запрещенных слов
	group := goka.DefineGroup(bw.config.Processor.GroupBadWord,
		goka.Input(bw.config.Topic.BadWords, new(codec.String), bw.badWordsUpdate),
		goka.Persist(codecBabWord),
	)

	p, err := goka.NewProcessor(bw.config.Brokers, group)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}
	defer p.Stop()

	bw.logger.Info("Starting processor...")
	if err = p.Run(ctx); err != nil {
		bw.logger.Info("Processor error: %v", err)
	}
}

func (bw *BadWord) badWordsUpdate(ctx goka.Context, msg any) {

	badWord, ok := msg.(string)
	if !ok {
		bw.logger.Error("badWords update: message is not a string: %T", msg)
		return
	}
	// Нормализуем слово
	badWord = strings.ToLower(strings.TrimSpace(badWord))
	if badWord == "" {
		return
	}

	var badWordsStore store.BadWordsStore
	if val := ctx.Value(); val != nil {
		badWordsStore, ok = val.(store.BadWordsStore)
		if !ok {
			bw.logger.Error("wrong store type: %T", val)
		}
	}

	badWordsStore.AddWord(badWord)
	ctx.SetValue(badWordsStore)
	bw.logger.Success("badWords updated: %#v", badWordsStore)
}
