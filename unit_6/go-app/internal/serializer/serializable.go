package serializer

import (
	"errors"
	"fmt"

	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/config"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

type Serializable[T any] interface {
	Serialize(topic string, data *T) ([]byte, error)
	Deserialize(topic string, data []byte, result *T) error
	Close() error
}

type serialize[T any] struct {
	serializer   serde.Serializer
	deserializer serde.Deserializer
	config       config.Config
	client       schemaregistry.Client
}

func (s *serialize[T]) Deserialize(topic string, data []byte, result *T) error {
	if s.deserializer == nil {
		return fmt.Errorf("deserializer is not initialized")
	}

	return s.deserializer.DeserializeInto(topic, data, result)
}
func (s *serialize[T]) Serialize(topic string, data *T) ([]byte, error) {
	if s.serializer == nil {
		return nil, fmt.Errorf("serializer is not initialized")
	}
	return s.serializer.Serialize(topic, data)
}

func (s *serialize[T]) Close() error {

	var cErr, sErr, dErr error

	if s.serializer != nil {
		sErr = s.serializer.Close()
		s.serializer = nil
	}
	if s.deserializer != nil {
		dErr = s.deserializer.Close()
		s.deserializer = nil
	}

	if s.client != nil {
		cErr = s.client.Close()
		s.client = nil
	}

	return errors.Join(sErr, dErr, cErr)
}

func (s *serialize[T]) schemaRegistryClient() (schemaregistry.Client, error) {
	configSchemaRegistry := schemaregistry.NewConfig(s.config.SchemaRegistry.URL)
	configSchemaRegistry.SslCertificateLocation = s.config.SchemaRegistry.SslCertificateLocation
	configSchemaRegistry.SslKeyLocation = s.config.SchemaRegistry.SslKeyLocation
	configSchemaRegistry.SslCaLocation = s.config.SchemaRegistry.SslCaLocation
	configSchemaRegistry.SslDisableEndpointVerification = s.config.SchemaRegistry.SslDisableEndpointVerification

	return schemaregistry.NewClient(configSchemaRegistry)
}
