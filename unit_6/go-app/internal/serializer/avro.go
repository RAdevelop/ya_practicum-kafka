package serializer

import (
	"errors"
	"fmt"

	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/config"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov3"
)

type Avro[T any] struct {
	client       schemaregistry.Client
	serializer   serde.Serializer
	deserializer serde.Deserializer
	config       config.Config
}

func NewAvro[T any](config config.Config) (*Avro[T], error) {

	configSchemaRegistry := schemaregistry.NewConfig(config.SchemaRegistry.URL)
	configSchemaRegistry.SslCertificateLocation = config.SchemaRegistry.SslCertificateLocation
	configSchemaRegistry.SslKeyLocation = config.SchemaRegistry.SslKeyLocation
	configSchemaRegistry.SslCaLocation = config.SchemaRegistry.SslCaLocation
	configSchemaRegistry.SslDisableEndpointVerification = config.SchemaRegistry.SslDisableEndpointVerification

	client, err := schemaregistry.NewClient(configSchemaRegistry)
	if err != nil {
		return nil, err
	}

	a := &Avro[T]{
		client: client,
	}

	serConfig := avrov3.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	a.serializer, err = avrov3.NewSerializer(client, serde.ValueSerde, serConfig)
	if err != nil {
		return nil, errors.Join(err, a.Close())
	}

	a.deserializer, err = avrov3.NewDeserializer(client, serde.ValueSerde, avrov3.NewDeserializerConfig())

	if err != nil {
		return nil, errors.Join(err, a.Close())
	}

	a.config = config

	return a, nil
}
func (a *Avro[T]) Close() error {

	var cErr, sErr, dErr error

	if a.serializer != nil {
		sErr = a.serializer.Close()
	}
	if a.deserializer != nil {
		dErr = a.deserializer.Close()
	}

	if a.client != nil {
		cErr = a.client.Close()
	}

	return errors.Join(sErr, dErr, cErr)
}

func (a *Avro[T]) Deserialize(topic string, data []byte, result *T) error {
	if a.deserializer == nil {
		return fmt.Errorf("deserializer is not initialized")
	}
	if result == nil {
		return fmt.Errorf("result cannot be nil")
	}
	return a.deserializer.DeserializeInto(topic, data, result)
}
func (a *Avro[T]) Serialize(topic string, data *T) ([]byte, error) {
	if a.serializer == nil {
		return nil, fmt.Errorf("serializer is not initialized")
	}
	return a.serializer.Serialize(topic, data)
}
