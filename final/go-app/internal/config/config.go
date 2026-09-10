package config

import (
	"crypto/tls"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/cert"
	"github.com/lovoo/goka"
	"github.com/struct0x/envconfig"
)

type Config struct {
	BootstrapServers string          `env:"BOOTSTRAP_SERVERS" envDefault:""`
	Topics           *topics         `envPrefix:"TOPIC"`
	KeyTopic         *keyTopic       `envPrefix:"KEY_TOPIC"`
	SchemaRegistry   *schemaRegistry `envPrefix:"SCHEMA_REGISTRY"`
	Shop             *shop           `envPrefix:"SHOP"`
	Client           *client         `envPrefix:"CLIENT"`
	ViewTable        *viewTable
	Processor        *processor `envPrefix:"PROCESSOR"`
	File             *file      `envPrefix:"FILE"`
}

func (c *Config) Load(envFilePath string) {
	if err := envconfig.Read(c, envconfig.EnvFileLookup(envFilePath)); err != nil {
		panic(err)
	}

	c.ViewTable.ProductsBlocked = goka.Table(c.Processor.GroupProductsBlocked + "-table")
	c.ViewTable.ProductsRecommendations = goka.Table(c.Topics.ProductsRecommendations)
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

type file struct {
	ProductsJson  string `env:"PRODUCTS_JSON" envDefault:""`
	ProductsJsonl string `env:"PRODUCTS_JSONL" envDefault:""`
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
