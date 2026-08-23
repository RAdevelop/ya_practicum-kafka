package serializer

import (
	"errors"
	"fmt"

	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/config"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

type Json[T any] struct {
	client       schemaregistry.Client
	serializer   serde.Serializer
	deserializer serde.Deserializer
	config       config.Config
}

func NewJson[T any](config config.Config) (*Json[T], error) {

	configSchemaRegistry := schemaregistry.NewConfig(config.SchemaRegistry.URL)
	configSchemaRegistry.SslCertificateLocation = config.SchemaRegistry.SslCertificateLocation
	configSchemaRegistry.SslKeyLocation = config.SchemaRegistry.SslKeyLocation
	configSchemaRegistry.SslCaLocation = config.SchemaRegistry.SslCaLocation
	configSchemaRegistry.SslDisableEndpointVerification = config.SchemaRegistry.SslDisableEndpointVerification

	client, err := schemaregistry.NewClient(configSchemaRegistry)
	if err != nil {
		return nil, err
	}

	j := &Json[T]{
		client: client,
	}

	serConfig := jsonschema.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.SubjectNameStrategyType = serde.TopicNameStrategyType
	j.serializer, err = jsonschema.NewSerializer(client, serde.ValueSerde, serConfig)
	if err != nil {
		return nil, errors.Join(err, j.Close())
	}

	dserConfig := jsonschema.NewDeserializerConfig()
	dserConfig.UseLatestVersion = true
	dserConfig.SubjectNameStrategyType = serde.TopicNameStrategyType
	j.deserializer, err = jsonschema.NewDeserializer(client, serde.ValueSerde, dserConfig)

	if err != nil {
		return nil, errors.Join(err, j.Close())
	}

	j.config = config

	return j, nil
}
func (j *Json[T]) Close() error {

	var cErr, sErr, dErr error

	if j.serializer != nil {
		sErr = j.serializer.Close()
	}
	if j.deserializer != nil {
		dErr = j.deserializer.Close()
	}

	if j.client != nil {
		cErr = j.client.Close()
	}

	return errors.Join(sErr, dErr, cErr)
}

func (j *Json[T]) Deserialize(topic string, data []byte, result *T) error {
	if j.deserializer == nil {
		return fmt.Errorf("deserializer is not initialized")
	}
	if result == nil {
		return fmt.Errorf("result cannot be nil")
	}
	return j.deserializer.DeserializeInto(topic, data, result)
}
func (j *Json[T]) Serialize(topic string, data *T) ([]byte, error) {
	if j.serializer == nil {
		return nil, fmt.Errorf("serializer is not initialized")
	}
	return j.serializer.Serialize(topic, data)
}
