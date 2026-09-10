package emitter

import (
	"strings"

	"github.com/IBM/sarama"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	"github.com/lovoo/goka"
)

type Products struct {
	shop
}

func NewProducts(config config.Config, codec goka.Codec) (*Products, error) {

	emitter, err := newEmitter(config.Topics.Products, config, codec)
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

	emitter, err := newEmitter(config.Topics.ProductsBlocked, config, codec)
	if err != nil {
		return nil, err
	}

	return &ProductsBlocked{
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

func newEmitter(topic string, config config.Config, codec goka.Codec) (*goka.Emitter, error) {
	tlsConfig, err := config.LoadShopConfigTLS()
	if err != nil {
		return nil, err
	}

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
