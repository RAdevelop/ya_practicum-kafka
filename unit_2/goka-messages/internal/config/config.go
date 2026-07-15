package config

import (
	"github.com/lovoo/goka"
	"github.com/struct0x/envconfig"
)

// Config - настройки для работы с эмиттерами, процессорами, view-таблицами
type Config struct {
	Brokers   []string   `env:"BROKERS" envDefault:"kafka-b-1:9092"`
	Processor *processor `envPrefix:"PROCESSOR"`
	Topic     *topic     `envPrefix:"TOPIC"`
	KeyTopic  *keyTopic  `envPrefix:"KEY_TOPIC"`
	ViewTable *viewTable
}

func (c *Config) Load(envFilePath string) {
	if err := envconfig.Read(c, envconfig.EnvFileLookup(envFilePath)); err != nil {
		panic(err)
	}

	c.ViewTable.CensorWord = goka.Table(c.Processor.GroupCensorWord + "-table")
	c.ViewTable.BlockedUsers = goka.Table(c.Processor.GroupBlockedUser + "-table")
}

type processor struct {
	GroupCensorWord  goka.Group `env:"GROUP_CENSOR_WORD" envDefault:"group-censor-word"`
	GroupBlockedUser goka.Group `env:"GROUP_BLOCKED_USERS" envDefault:"group-blocked-users"`
	GroupSender      goka.Group `env:"GROUP_SENDER" envDefault:"group-sender"`
}
type topic struct {
	Messages         goka.Stream `env:"MESSAGES" envDefault:"messages"`
	FilteredMessages goka.Stream `env:"FILTERED_MESSAGES" envDefault:"filtered-messages"`
	BlockedUsers     goka.Stream `env:"BLOCKED_USERS" envDefault:"blocked-users"`
	BadWords         goka.Stream `env:"BAD_WORDS" envDefault:"bad-words"`
}
type keyTopic struct {
	BadWords string `env:"BAD_WORDS" envDefault:"bad-word"`
}

type viewTable struct {
	CensorWord   goka.Table `env:"CENSOR_WORD"`
	BlockedUsers goka.Table `env:"BLOCKED_USERS"`
}
