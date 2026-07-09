package config

import (
	"github.com/struct0x/envconfig"
)

// Config - TODO описать правильно структуру
type Config struct {
	Emitter   *emitter   `envPrefix:"EMITTER"`
	Processor *processor `envPrefix:"PROCESSOR"`
	Topic     *topic     `envPrefix:"TOPIC"`
}

func (c *Config) Load(envFilePath string) {
	if err := envconfig.Read(c, envconfig.EnvFileLookup(envFilePath)); err != nil {
		panic(err)
	}
}

type emitter struct {
	Brokers string `env:"BROKERS" envDefault:"kafka-b-1:9092"`
}

type processor struct {
	Brokers          string `env:"BROKERS" envDefault:"kafka-b-1:9092"`
	GroupCensorWord  string `env:"GROUP_CENSOR_WORD" envDefault:"group-censor-word"`
	GroupBlockedUser string `env:"GROUP_BLOCKED_USERS" envDefault:"group-blocked-user"`
	GroupLogger      string `env:"GROUP_LOGGER" envDefault:"group-logger"`
}
type topic struct {
	Messages                     string `env:"MESSAGES" envDefault:"messages"`
	FilteredMessages             string `env:"FILTERED_MESSAGES" envDefault:"filtered_messages"`
	BlockedUsers                 string `env:"BLOCKED_USERS" envDefault:"blocked_users"`
	BadWords                     string `env:"BAD_WORDS" envDefault:"bad_words"`
	MessagesNeedsCheckedByCensor string `env:"MESSAGES_NEEDS_CHECKED_BY_CENSOR" envDefault:"messages_needs_checked_by_censor"`
}
