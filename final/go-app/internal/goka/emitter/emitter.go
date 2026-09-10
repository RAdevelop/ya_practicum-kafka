package emitter

import (
	"crypto/tls"
	"strings"

	"github.com/IBM/sarama"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	"github.com/lovoo/goka"
)

type Products struct {
	shop
}

func NewProducts(config config.Config, codec goka.Codec) (*Products, error) {

	tlsConfig, err := config.LoadShopConfigTLS()
	if err != nil {
		return nil, err
	}

	emitter, err := newEmitter(config.Topics.Products, config, codec, tlsConfig)
	if err != nil {
		return nil, err
	}

	return &Products{
		shop: shop{emitter: emitter},
	}, nil
}

type ProductsBlocked struct {
	shop
}

func NewProductsBlocked(config config.Config, codec goka.Codec) (*ProductsBlocked, error) {

	tlsConfig, err := config.LoadShopConfigTLS()
	if err != nil {
		return nil, err
	}

	emitter, err := newEmitter(config.Topics.ProductsBlocked, config, codec, tlsConfig)
	if err != nil {
		return nil, err
	}

	return &ProductsBlocked{
		shop: shop{emitter: emitter},
	}, nil
}

type ClientSearch struct {
	shop
}

func NewClientSearch(config config.Config, codec goka.Codec) (*ClientSearch, error) {

	tlsConfig, err := config.LoadClientConfigTLS()
	if err != nil {
		return nil, err
	}

	emitter, err := newEmitter(config.Topics.ClientSearch, config, codec, tlsConfig)
	if err != nil {
		return nil, err
	}

	return &ClientSearch{
		shop: shop{emitter: emitter},
	}, nil
}

type shop struct {
	emitter *goka.Emitter
}

func (em *shop) Finish() error {

	if em != nil && em.emitter != nil {
		return em.emitter.Finish()
	}
	return nil
}

func (em *shop) EmitSync(key string, msg interface{}) error {
	return em.emitter.EmitSync(key, msg)
}

func newEmitter(topic string, config config.Config, codec goka.Codec, tlsConfig *tls.Config) (*goka.Emitter, error) {

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = tlsConfig
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	producerConfig := goka.ProducerBuilderWithConfig(saramaConfig)
	brokers := strings.Split(config.BootstrapServers, ",")
	emitter, err := goka.NewEmitter(brokers, goka.Stream(topic), codec, goka.WithEmitterProducerBuilder(producerConfig))
	if err != nil {
		return nil, err
	}

	return emitter, nil
}
