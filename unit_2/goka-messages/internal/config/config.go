package config

import (
	"github.com/struct0x/envconfig"
)

// Config - TODO описать правильно структуру
type Config struct {
	Producer *producer `envPrefix:"PRODUCER"`
	Consumer *consumer `envPrefix:"CONSUMER"`
}

func (c *Config) Load(envFilePath string) {
	if err := envconfig.Read(c, envconfig.EnvFileLookup(envFilePath)); err != nil {
		panic(err)
	}
}

type producer struct {
	BootstrapServers               string `env:"BOOTSTRAP_SERVERS" envDefault:"kafka-b-1:9092"`
	Acks                           string `env:"ACKS" envDefault:"all"`
	Retries                        int    `env:"RETRIES" envDefault:"10"`
	RetryBackoffMs                 int    `env:"RETRY_BACKOFF_MS" envDefault:"100"`
	EnableIdempotence              bool   `env:"ENABLE_IDEMPOTENCE" envDefault:"false"`
	FlushTimeoutMs                 int    `env:"FLUSH_TIMEOUT_MS" envDefault:"15000"`
	SocketConnectionSetupTimeoutMs int    `env:"SOCKET_CONNECTION_SETUP_TIMEOUT_MS" envDefault:"10000"`
	SocketTimeoutMs                int    `env:"SOCKET_TIMEOUT_MS" envDefault:"30000"`
}

type consumer struct {
	BootstrapServers      string `env:"BOOTSTRAP_SERVERS" envDefault:"kafka-b-1:9092"`
	GroupId               string `env:"GROUP_ID" envDefault:""`
	AutoOffsetReset       string `env:"AUTO_OFFSET_RESET" envDefault:"earliest"`
	EnableAutoCommit      bool   `env:"ENABLE_AUTO_COMMIT" envDefault:"false"`
	EnableAutoOffsetStore bool   `env:"ENABLE_AUTO_OFFSET_STORE" envDefault:"false"`
	FetchMinBytes         int    `env:"FETCH_MIN_BYTES" envDefault:"1024"`
	FetchWaitMaxMs        int    `env:"FETCH_WAIT_MAX_MS" envDefault:"100"`
}
