package emitter

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	"github.com/lovoo/goka"
)

type Products struct {
	emitter *goka.Emitter
}

func NewProducts(config config.Config, codec goka.Codec) (*Products, error) {

	tlsConfig, err := loadTLSConfig(config.Shop.SslCaLocation, config.Shop.SslCertLocation, config.Shop.SslCertificatePK8)
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
	emitter, err := goka.NewEmitter(brokers, goka.Stream(config.Topics.Products), codec, goka.WithEmitterProducerBuilder(producerConfig))
	if err != nil {
		return nil, err
	}

	return &Products{
		emitter: emitter,
	}, nil
}

func (em *Products) Finish() error {
	return em.emitter.Finish()
}

func (em *Products) EmitSync(key string, msg interface{}) error {
	return em.emitter.EmitSync(key, msg)
}

func loadTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("parse CA: %w", err)
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}, nil
}
