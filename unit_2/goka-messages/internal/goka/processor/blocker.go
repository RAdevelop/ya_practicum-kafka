package processor

import (
	"context"
	"strings"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	jsCode "github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/store"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

type BlockProcessor struct {
	logger *logger.Logger
	config config.Config
}

// NewUserBlocker — конструктор
func NewUserBlocker(config config.Config) *BlockProcessor {
	return &BlockProcessor{
		logger: logger.New("[BlockProcessor]"),
		config: config,
	}
}

// Run — запуск процессора
func (b *BlockProcessor) Run(ctx context.Context) {
	// Определяем группу процессора
	group := goka.DefineGroup(b.config.Processor.GroupBlockedUser,
		goka.Input(b.config.Topic.BlockedUsers, new(codec.String), b.processBlockEvent),
		goka.Persist(new(jsCode.JsonCodec[store.BlockedUsersStore])),
	)

	p, err := goka.NewProcessor(b.config.Brokers, group)
	if err != nil {
		b.logger.Error("Failed to create processor: %v", err)
		return
	}
	defer func() {
		p.Stop()
		b.logger.Info("Processor stopped")
	}()

	b.logger.Info("Starting processor...")
	if err = p.Run(ctx); err != nil {
		b.logger.Error("Processor error: %v", err)
	}
}

// processBlockEvent — обработчик событий блокировки
func (b *BlockProcessor) processBlockEvent(ctx goka.Context, msg any) {
	blockEvent, correctType := msg.(string)
	if !correctType {
		b.logger.Error("wrong message type: %T", msg)
		return
	}

	parts := strings.Split(blockEvent, ":")
	if len(parts) != 3 {
		b.logger.Error("invalid format: %s", blockEvent)
		return
	}

	action := parts[0]
	blockerID := parts[1]
	blockedID := parts[2]

	// Читаем текущее состояние
	var storeBlockedUsers store.BlockedUsersStore
	if val := ctx.Value(); val != nil {
		var ok bool
		storeBlockedUsers, ok = val.(store.BlockedUsersStore)
		if !ok {
			b.logger.Error("wrong store type: %T", val)
			storeBlockedUsers = store.NewBlockedUsersStore(blockerID)
		}
	} else {
		storeBlockedUsers = store.NewBlockedUsersStore(blockerID)
	}

	// Обновляем список
	switch action {
	case "block":
		storeBlockedUsers.Block(blockedID)
	case "unblock":
		storeBlockedUsers.Unblock(blockedID)
	default:
		b.logger.Error("unknown action: %s", action)
		return
	}

	// Сохраняем состояние
	ctx.SetValue(storeBlockedUsers)
	// в логах видим агрегированное состояние по пользователям
	b.logger.Success("Blocked users for ctx.Key()= %s, ctx.Value() = %v", ctx.Key(), ctx.Value())
	b.logger.Success("Blocked users for %s: %v", blockerID, storeBlockedUsers.BlockedUserIDs)
}
