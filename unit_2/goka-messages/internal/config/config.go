package config

import (
	"github.com/lovoo/goka"
	"github.com/struct0x/envconfig"
)

// Config - TODO описать правильно структуру
type Config struct {
	Brokers   []string   `env:"BROKERS" envDefault:"kafka-b-1:9092"`
	Processor *processor `envPrefix:"PROCESSOR"`
	Topic     *topic     `envPrefix:"TOPIC"`
}

func (c *Config) Load(envFilePath string) {
	if err := envconfig.Read(c, envconfig.EnvFileLookup(envFilePath)); err != nil {
		panic(err)
	}
}

type processor struct {
	GroupCensorWord  goka.Group `env:"GROUP_CENSOR_WORD" envDefault:"group-censor-word"`
	GroupBlockedUser goka.Group `env:"GROUP_BLOCKED_USERS" envDefault:"group-blocked-user"`
	GroupSender      goka.Group `env:"GROUP_SENDER" envDefault:"group-sender"`
}
type topic struct {
	Messages                     goka.Stream `env:"MESSAGES" envDefault:"messages"`
	FilteredMessages             goka.Stream `env:"FILTERED_MESSAGES" envDefault:"filtered_messages"`
	BlockedUsers                 goka.Stream `env:"BLOCKED_USERS" envDefault:"blocked_users"`
	BadWords                     goka.Stream `env:"BAD_WORDS" envDefault:"bad_words"`
	MessagesNeedsCheckedByCensor goka.Stream `env:"MESSAGES_NEEDS_CHECKED_BY_CENSOR" envDefault:"messages_needs_checked_by_censor"`
}
