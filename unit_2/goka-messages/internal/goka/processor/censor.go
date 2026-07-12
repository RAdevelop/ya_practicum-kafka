package processor

import (
	"context"
	"log"
	"regexp"
	"strings"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

// BannedWordsStore — хранилище запрещенных слов
type badWordsStore struct {
	Words map[string]string `json:"words"`
}

// AddWord - добавляем новое слово в список запрещенных
func (s *badWordsStore) AddWord(word string) {

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
func (s *badWordsStore) GetMask(word string) (string, bool) {
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

// Cens - запуск процесс применения цензуры
func (c *Censor) Cens(ctx context.Context) {
	codecBabWord := new(jsCodec.JsonCodec[badWordsStore])

	// Создаем View для чтения групповой таблицы (вот такая особенность работы с View)
	// Это чтобы можно было в методах обработчиках получать доступ к значению карты запрещенных слов, что сохраняем в персистентной таблице
	table := goka.Table(c.config.Processor.GroupCensorWord + "-table")
	view, err := goka.NewView(
		c.config.Brokers,
		table,
		codecBabWord,
	)
	if err != nil {
		c.logger.Error("Failed to create view: %v", err)
		return
	}

	// Запускаем View в отдельной горутине, чтобы view.Run не блокировал следующий код
	go func() {
		c.logger.Info("Starting View...")
		if err := view.Run(ctx); err != nil {
			c.logger.Error("View error: %v", err)
		}
	}()

	// определяем группу для ценузры
	group := goka.DefineGroup(c.config.Processor.GroupCensorWord,
		goka.Input(c.config.Topic.BadWords, new(codec.String), c.badWordsUpdate),
		goka.Input(c.config.Topic.Messages, new(jsCodec.JsonCodec[model.Message]), c.createMessageHandler(view)),
		goka.Persist(codecBabWord),
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

	var store badWordsStore
	if val := ctx.Value(); val != nil {
		store, ok = val.(badWordsStore)
		if !ok {
			c.logger.Error("wrong store type: %T", val)
		}
	}

	store.AddWord(badWord)
	ctx.SetValue(store)
	c.logger.Success("badWords updated: %#v", store)
}

// createMessageHandler создает обработчик с доступом к View для карты с запрещенными словами
func (c *Censor) createMessageHandler(view *goka.View) func(ctx goka.Context, msg interface{}) {
	return func(ctx goka.Context, msg interface{}) {
		c.processCensForMessage(ctx, msg, view)
	}
}

// processCensForMessage - фильтруем текст сообщения по запрещенным словам, они будут заменены на маску "*"
func (c *Censor) processCensForMessage(ctx goka.Context, msg interface{}, view *goka.View) {
	defer c.logger.Info("processCensForMessage finished\n")

	c.logger.Info("processCensForMessage started\n")

	message, ok := msg.(model.Message)
	if !ok {
		c.logger.Error("wrong message type: %T\n", msg)
		return
	}

	mapBadWord, err := view.Get("bad_word") //TODO bad_word - надо вынести в конфиг
	if err != nil {
		c.logger.Error("failed to get banned words: %v", err)
		// продолжаем без цензуры
		ctx.Emit(c.config.Topic.FilteredMessages, message.IDToString(), message)
		return
	}

	c.logger.Info("mapBadWord = %#v", mapBadWord)

	var store badWordsStore
	if store, ok = mapBadWord.(badWordsStore); !ok {
		c.logger.Error("wrong store type: %T", mapBadWord)
		ctx.Emit(c.config.Topic.FilteredMessages, message.IDToString(), message)
		return
	}

	// Применяем цензуру
	message.Text = c.replaceBadWordWithMask(message.Text, store)

	c.logger.Info("process messageID= %s, %#v", message.IDToString(), message)
	ctx.Emit(c.config.Topic.FilteredMessages, message.IDToString(), message)
}

func (c *Censor) replaceBadWordWithMask(text string, store badWordsStore) string {
	if len(store.Words) == 0 {
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

	c.logger.Info("replaceBadWordWithMask start")

	result := re.ReplaceAllStringFunc(text, func(word string) string {
		lowerWord := strings.ToLower(word)
		if mask, exists := store.Words[lowerWord]; exists {
			// Сохраняем регистр? Пока просто заменяем на маску
			return mask
		}
		return word
	})

	return result
}
