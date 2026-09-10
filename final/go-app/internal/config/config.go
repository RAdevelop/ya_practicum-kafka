package config

import (
	"crypto/tls"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/cert"
	"github.com/lovoo/goka"
	"github.com/struct0x/envconfig"
)

type Config struct {
	BootstrapServers string          `env:"BOOTSTRAP_SERVERS" envDefault:""`
	Producer         *producer       `envPrefix:"PRODUCER"`
	Consumer         *consumer       `envPrefix:"CONSUMER"`
	Topics           *topics         `envPrefix:"TOPIC"`
	KeyTopic         *keyTopic       `envPrefix:"KEY_TOPIC"`
	SchemaRegistry   *schemaRegistry `envPrefix:"SCHEMA_REGISTRY"`
	Shop             *shop           `envPrefix:"SHOP"`
	Client           *client         `envPrefix:"CLIENT"`
	ViewTable        *viewTable
	Processor        *processor `envPrefix:"PROCESSOR"`
}

func (c *Config) Load(envFilePath string) {
	if err := envconfig.Read(c, envconfig.EnvFileLookup(envFilePath)); err != nil {
		panic(err)
	}

	c.ViewTable.ProductsBlocked = goka.Table(c.Processor.GroupProductsBlocked + "-table")
	c.ViewTable.ProductsRecommendations = goka.Table(c.Topics.ProductsRecommendations)
}

type producer struct {
	Debug                            string `env:"Debug" envDefault:""`
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
	Products                string `env:"PRODUCTS" envDefault:""`
	ProductsBlocked         string `env:"PRODUCTS_BLOCKED" envDefault:""`
	ProductsPublished       string `env:"PRODUCTS_PUBLISHED" envDefault:""`
	ProductsRecommendations string `env:"PRODUCTS_RECOMMENDATIONS" envDefault:""`
	ClientSearch            string `env:"CLIENT_SEARCH" envDefault:""`
}

type keyTopic struct {
	ProductsBlocked string `env:"PRODUCTS_BLOCKED" envDefault:"products-blocked"`
}

type schemaRegistry struct {
	URL                            string `env:"URL"`
	SslCertificateLocation         string `env:"SSL_CERTIFICATE_LOCATION"`
	SslCaLocation                  string `env:"SSL_CA_LOCATION"`
	SslKeyLocation                 string `env:"SSL_KEY_LOCATION"`
	SslDisableEndpointVerification bool   `env:"SSL_DISABLE_ENDPOINT_VERIFICATION"`
}

type shop struct {
	SslCaLocation     string `env:"SSL_CA_LOCATION"`
	SslCertLocation   string `env:"SSL_CERTIFICATE_LOCATION"`
	SslCertificatePK8 string `env:"SSL_CERTIFICATE_PK8"`
}

type client struct {
	SslCaLocation     string `env:"SSL_CA_LOCATION"`
	SslCertLocation   string `env:"SSL_CERTIFICATE_LOCATION"`
	SslCertificatePK8 string `env:"SSL_CERTIFICATE_PK8"`
}

type viewTable struct {
	ProductsBlocked         goka.Table `env:"PRODUCTS_BLOCKED"`
	ProductsRecommendations goka.Table `env:"PRODUCTS_RECOMMENDATIONS"`
}

type processor struct {
	GroupProductsBlocked         goka.Group `env:"GROUP_PRODUCTS_BLOCKED" envDefault:"group-products-blocked"`
	GroupProductsCensor          goka.Group `env:"GROUP_PRODUCTS_CENSOR" envDefault:"group-products-censor"`
	GroupProductsRecommendations goka.Group `env:"GROUP_PRODUCTS_RECOMMENDATIONS" envDefault:"group-products-recommendations"`
}

func (c *Config) LoadShopConfigTLS() (*tls.Config, error) {
	return cert.LoadTLSConfig(
		c.Shop.SslCaLocation,
		c.Shop.SslCertLocation,
		c.Shop.SslCertificatePK8,
	)
}

func (c *Config) LoadClientConfigTLS() (*tls.Config, error) {
	return cert.LoadTLSConfig(
		c.Client.SslCaLocation,
		c.Client.SslCertLocation,
		c.Client.SslCertificatePK8,
	)
}
