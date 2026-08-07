package config

import "github.com/struct0x/envconfig"

type Config struct {
    Consumer *consumer `envPrefix:"CONSUMER"`
}

func (c *Config) Load(envFilePath string) {
    if err := envconfig.Read(c, envconfig.EnvFileLookup(envFilePath)); err != nil {
        panic(err)
    }
}

type consumer struct {
    BootstrapServers      string `env:"BOOTSTRAP_SERVERS" envDefault:"kafka-b-1:9092"`
    GroupIdUsers          string `env:"GROUP_ID_USERS" envDefault:"pg_public_users"`
    GroupIdOrders         string `env:"GROUP_ID_ORDERS" envDefault:"pg_public_orders"`
    AutoOffsetReset       string `env:"AUTO_OFFSET_RESET" envDefault:"earliest"`
    EnableAutoCommit      bool   `env:"ENABLE_AUTO_COMMIT" envDefault:"false"`
    EnableAutoOffsetStore bool   `env:"ENABLE_AUTO_OFFSET_STORE" envDefault:"false"`
    FetchMinBytes         int    `env:"FETCH_MIN_BYTES" envDefault:"1024"`
    FetchWaitMaxMs        int    `env:"FETCH_WAIT_MAX_MS" envDefault:"100"`
}
