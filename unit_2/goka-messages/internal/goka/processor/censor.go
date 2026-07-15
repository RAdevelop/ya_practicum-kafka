package processor

import (
	"context"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/api"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/store"
	"github.com/lovoo/goka"
)

type Censor struct {
	logger *logger.Logger
	config config.Config
	views  *api.Views
}

func NewCensor(config config.Config, views *api.Views) *Censor {
	return &Censor{
		logger: logger.New("[CensorProcessor]"),
		config: config,
		views:  views,
	}
}

// Run - запуск процесс применения цензуры
func (c *Censor) Run(ctx context.Context) {
	// определяем группу для цензуры
	group := goka.DefineGroup(c.config.Processor.GroupCensorWord,
		goka.Input(c.config.Topic.Messages, new(jsCodec.JsonCodec[model.Message]), c.processCensForMessage),
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

// processCensForMessage - фильтруем текст сообщения по запрещенным словам, они будут заменены на маску "*"
func (c *Censor) processCensForMessage(ctx goka.Context, msg any) {
	message, ok := msg.(model.Message)
	if !ok {
		c.logger.Error("wrong message type: %T\n", msg)
		// наверное, тут надо складывать такие сообщения в DQL-топик
		return
	}

	toUserID := strconv.FormatInt(int64(message.ToUserID), 10)
	fromUserID := strconv.FormatInt(int64(message.FromUserID), 10)
	blockedUser, err := c.views.BlockedUserView.Get(toUserID)
	if err != nil {
		c.logger.Error("failed to get banned users for userID: %d, err: %v", message.ToUserID, err)
	}

	c.logger.Info("blockedUser = %#v", blockedUser)

	var blockedUsersStore *store.BlockedUsersStore
	blockedUsersStore, ok = blockedUser.(*store.BlockedUsersStore)
	if !ok {
		c.logger.Error("wrong store type: %T", blockedUser)
	} else if _, exists := blockedUsersStore.BlockedUserIDs[fromUserID]; exists {
		c.logger.Error("can't send message FromUserID to ToUserID, cause FromUserID is blocked, message=%v", message)
		return
	}

	mapBadWord, err := c.views.BadWordsView.Get(c.config.KeyTopic.BadWords)
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

	c.logger.Success("process messageID= %s, %+v", message.IDToString(), message)
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
