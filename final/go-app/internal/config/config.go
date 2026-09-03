package config

import "github.com/struct0x/envconfig"

type Config struct {
	Producer       *producer       `envPrefix:"PRODUCER"`
	Consumer       *consumer       `envPrefix:"CONSUMER"`
	Topics         *topics         `envPrefix:"TOPIC"`
	SchemaRegistry *schemaRegistry `envPrefix:"SCHEMA_REGISTRY"`
}

func (c *Config) Load(envFilePath string) {
	if err := envconfig.Read(c, envconfig.EnvFileLookup(envFilePath)); err != nil {
		panic(err)
	}
}

type producer struct {
	Debug                            string `env:"Debug" envDefault:""`
	BootstrapServers                 string `env:"BOOTSTRAP_SERVERS" envDefault:""`
	Acks                             string `env:"ACKS" envDefault:"all"`
	Retries                          int    `env:"RETRIES" envDefault:"10"`
	RetryBackoffMs                   int    `env:"RETRY_BACKOFF_MS" envDefault:"100"`
	EnableIdempotence                bool   `env:"ENABLE_IDEMPOTENCE" envDefault:"true"`
	MaxInFlightRequestsPerConnection int    `env:"MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION" envDefault:"5"`
	FlushTimeoutMs                   int    `env:"FLUSH_TIMEOUT_MS" envDefault:"15000"`
	SocketConnectionSetupTimeoutMs   int    `env:"SOCKET_CONNECTION_SETUP_TIMEOUT_MS" envDefault:"10000"`
	SocketTimeoutMs                  int    `env:"SOCKET_TIMEOUT_MS" envDefault:"30000"`

	SecurityProtocol       string `env:"SECURITY_PROTOCOL"`
	SslCaLocation          string `env:"SSL_CA_LOCATION"`
	SslCertificateLocation string `env:"SSL_CERTIFICATE_LOCATION"`
	SslKeyLocation         string `env:"SSL_KEY_LOCATION"`
	SslKeyPassword         string `env:"SSL_KEY_PASSWORD"`
}

type consumer struct {
	Debug                 string `env:"Debug" envDefault:""`
	BootstrapServers      string `env:"BOOTSTRAP_SERVERS" envDefault:"kafka-b-1:9093"`
	GroupId               string `env:"GROUP_ID" envDefault:""`
	AutoOffsetReset       string `env:"AUTO_OFFSET_RESET" envDefault:"earliest"`
	EnableAutoCommit      bool   `env:"ENABLE_AUTO_COMMIT" envDefault:"false"`
	EnableAutoOffsetStore bool   `env:"ENABLE_AUTO_OFFSET_STORE" envDefault:"false"`
	FetchMinBytes         int    `env:"FETCH_MIN_BYTES" envDefault:"1024"`
	FetchWaitMaxMs        int    `env:"FETCH_WAIT_MAX_MS" envDefault:"100"`

	SecurityProtocol       string `env:"SECURITY_PROTOCOL"`
	SslCaLocation          string `env:"SSL_CA_LOCATION"`
	SslCertificateLocation string `env:"SSL_CERTIFICATE_LOCATION"`
	SslKeyLocation         string `env:"SSL_KEY_LOCATION"`
	SslKeyPassword         string `env:"SSL_KEY_PASSWORD"`
}

type topics struct {
	ShopProducts string `env:"SHOP_PRODUCTS" envDefault:""`
}

type schemaRegistry struct {
	URL                            string `env:"URL"`
	SslCertificateLocation         string `env:"SSL_CERTIFICATE_LOCATION"`
	SslCaLocation                  string `env:"SSL_CA_LOCATION"`
	SslKeyLocation                 string `env:"SSL_KEY_LOCATION"`
	SslDisableEndpointVerification bool   `env:"SSL_DISABLE_ENDPOINT_VERIFICATION"`
}
