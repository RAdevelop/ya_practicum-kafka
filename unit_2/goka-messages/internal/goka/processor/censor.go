package processor

import (
	"context"
	"log"
	"regexp"
	"strings"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/view"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/store"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

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

// Run - запуск процесс применения цензуры
func (c *Censor) Run(ctx context.Context) {
	codecBabWord := new(jsCodec.JsonCodec[store.BadWordsStore])

	viewBadWords, err := view.NewView(ctx, c.config.ViewTable.CensorWord, codecBabWord, c.config, c.logger)
	if err != nil {
		c.logger.Error("Failed to create view for BadWords: %v", err)
		return
	}
	c.logger.Info("View for BadWords is ready")

	// определяем группу для цензуры
	group := goka.DefineGroup(c.config.Processor.GroupCensorWord,
		goka.Input(c.config.Topic.BadWords, new(codec.String), c.badWordsUpdate),
		goka.Input(c.config.Topic.Messages, new(jsCodec.JsonCodec[model.Message]), c.createMessageHandler(viewBadWords)),
		goka.Persist(codecBabWord),
		goka.Output(c.config.Topic.FilteredMessages, new(jsCodec.JsonCodec[model.Message])),
	)

	p, err := goka.NewProcessor(c.config.Brokers, group)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}
	defer p.Stop()

	c.logger.Info("Starting processor...")
	if err = p.Run(ctx); err != nil {
		c.logger.Info("Processor error: %v", err)
	}
}

func (c *Censor) badWordsUpdate(ctx goka.Context, msg any) {

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

	var badWordsStore store.BadWordsStore
	if val := ctx.Value(); val != nil {
		badWordsStore, ok = val.(store.BadWordsStore)
		if !ok {
			c.logger.Error("wrong store type: %T", val)
		}
	}

	badWordsStore.AddWord(badWord)
	ctx.SetValue(badWordsStore)
	c.logger.Success("badWords updated: %#v", badWordsStore)
}

// createMessageHandler создает обработчик с доступом к View для карты с запрещенными словами
func (c *Censor) createMessageHandler(viewBadWords *goka.View) func(ctx goka.Context, msg any) {
	return func(ctx goka.Context, msg any) {
		c.processCensForMessage(ctx, msg, viewBadWords)
	}
}

// processCensForMessage - фильтруем текст сообщения по запрещенным словам, они будут заменены на маску "*"
func (c *Censor) processCensForMessage(ctx goka.Context, msg any, viewBadWords *goka.View) {
	message, ok := msg.(model.Message)
	if !ok {
		c.logger.Error("wrong message type: %T\n", msg)
		// наверное, тут надо складывать такие сообщения в DQL-топик
		return
	}

	mapBadWord, err := viewBadWords.Get(c.config.KeyTopic.BadWords)
	if err != nil {
		c.logger.Error("failed to get banned words: %v", err)
		// продолжаем без цензуры
		ctx.Emit(c.config.Topic.FilteredMessages, message.IDToString(), message)
		return
	}

	c.logger.Info("mapBadWord = %#v", mapBadWord)

	var badWordsStore store.BadWordsStore
	if badWordsStore, ok = mapBadWord.(store.BadWordsStore); !ok {
		c.logger.Error("wrong store type: %T", mapBadWord)
		// продолжаем без цензуры
		ctx.Emit(c.config.Topic.FilteredMessages, message.IDToString(), message)
		return
	}

	// Применяем цензуру
	message.Text = c.replaceBadWordWithMask(message.Text, badWordsStore.Words)

	c.logger.Success("process messageID= %s, %#v", message.IDToString(), message)
	ctx.Emit(c.config.Topic.FilteredMessages, message.IDToString(), message)
}

// replaceBadWordWithMask - замена слов в тексте на их маски
// вообще, такой метод лучше вынести в отдельный пакет/класс, так как в целом, его можно будет использоваться не только для процесса цензуры
func (c *Censor) replaceBadWordWithMask(text string, wordsStore map[string]string) string {
	if len(wordsStore) == 0 {
		return text
	}

	// Разбиваем текст на слова
	words := strings.Fields(text)
	if len(words) == 0 {
		return text
	}
	// Заменяем только слова, сохраняя все остальное (пробелы, пунктуацию)
	// Создаем регулярное выражение для поиска всех слов
	// \p{L} — любая буква Unicode
	re := regexp.MustCompile(`\p{L}+`)

	result := re.ReplaceAllStringFunc(text, func(word string) string {
		lowerWord := strings.ToLower(word)
		if mask, exists := wordsStore[lowerWord]; exists {
			// Сохраняем регистр? Пока просто заменяем на маску
			return mask
		}
		return word
	})

	return result
}
