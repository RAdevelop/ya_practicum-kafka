package processor

import (
	"context"
	"log"
	"strings"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

// BannedWordsStore — хранилище запрещенных слов
type bannedWordsStore struct {
	Words map[string]string `json:"words"`
}

// AddWord - добавляем новое слово в список запрещенных
func (s *bannedWordsStore) AddWord(word string) {

	word = strings.TrimSpace(word)

	if word == "" {
		return
	}

	word = strings.ToLower(word)

	if s.Words == nil {
		s.Words = make(map[string]string)
	}

	if _, exists := s.Words[word]; exists {
		return
	}

	s.Words[word] = (func(word string) string {
		return strings.Repeat("*", len([]rune(word)))
	})(word)
}

// GetMask - получаем маску для указанного слова
func (s *bannedWordsStore) GetMask(word string) (string, bool) {
	if s.Words == nil {
		return "", false
	}
	word = strings.TrimSpace(word)
	if word == "" {
		return "", false
	}
	word = strings.ToLower(word)
	if mask, exists := s.Words[word]; exists {
		return mask, exists
	}
	return "", false
}

type Censor struct {
	logger *logger.Logger
	config config.Config
}

func NewCensor(config config.Config) *Censor {
	return &Censor{
		logger: logger.New("[CensorProcessor]"),
		config: config,
	}
}

func (c *Censor) Cens(ctx context.Context) {
	group := goka.DefineGroup(c.config.Processor.GroupCensorWord,
		goka.Input(c.config.Topic.BadWords, new(codec.String), c.badWordsUpdate),
		goka.Input(c.config.Topic.Messages, new(jsCodec.JsonCodec[model.Message]), c.processCensForMessage),
		goka.Persist(new(jsCodec.JsonCodec[bannedWordsStore])),
		goka.Output(c.config.Topic.FilteredMessages, new(jsCodec.JsonCodec[model.Message])),
	)

	p, err := goka.NewProcessor(c.config.Brokers, group)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}
	defer p.Stop()

	log.Println("Starting processor...")
	if err := p.Run(ctx); err != nil {
		log.Printf("Processor error: %v", err)
	}
}

func (c *Censor) badWordsUpdate(ctx goka.Context, msg interface{}) {

	defer c.logger.Info("badWordsUpdate finished\n")

	c.logger.Info("badWordsUpdate start\n")
	badWord, ok := msg.(string)
	if !ok {
		c.logger.Error("badWords update: message is not a string: %T", msg)
		return
	}
	// Нормализуем слово
	badWord = strings.ToLower(strings.TrimSpace(badWord))
	if badWord == "" {
		return
	}

	var store bannedWordsStore
	if val := ctx.Value(); val != nil {
		store, ok = val.(bannedWordsStore)
		if !ok {
			c.logger.Error("wrong store type: %T", val)
		}
	}

	store.AddWord(badWord)
	ctx.SetValue(store)
	c.logger.Success("badWords updated: %#v", store)
}
func (c *Censor) processCensForMessage(ctx goka.Context, msg interface{}) {
	defer c.logger.Info("processCensForMessage finished\n")

	c.logger.Info("processCensForMessage started\n")
	c.logger.Info(" ctx.Key() = %s, ctx.Value() = %#v\n", ctx.Key(), ctx.Value())
	//return
	message, ok := msg.(model.Message)
	if !ok {
		c.logger.Error("wrong message type: %T\n", msg)
		return
	}

	c.logger.Info("process messageID= %s, %#v", message.IDToString(), msg)
	ctx.Emit(c.config.Topic.FilteredMessages, message.IDToString(), msg)
}
