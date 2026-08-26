package serializer

import (
	"errors"

	"github.com/RAdevelop/ya_practicum-kafka/unit_6/go-app/internal/config"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

type Json[T any] struct {
	serialize[T]
}

func NewJson[T any](config config.Config) (*Json[T], error) {

	serializer := &Json[T]{}
	serializer.config = config

	var err error
	serializer.client, err = serializer.schemaRegistryClient()
	if err != nil {
		return nil, errors.Join(err, serializer.Close())
	}

	jsonSerializerConfig := jsonschema.NewSerializerConfig()
	jsonSerializerConfig.AutoRegisterSchemas = false
	jsonSerializerConfig.UseLatestVersion = true
	jsonSerializerConfig.SubjectNameStrategyType = serde.TopicNameStrategyType

	serializer.serializer, err = jsonschema.NewSerializer(serializer.client, serde.ValueSerde, jsonSerializerConfig)

	if err != nil {
		return nil, errors.Join(err, serializer.Close())
	}

	jsonDeserializerConfig := jsonschema.NewDeserializerConfig()
	jsonDeserializerConfig.UseLatestVersion = true
	jsonDeserializerConfig.SubjectNameStrategyType = serde.TopicNameStrategyType

	serializer.deserializer, err = jsonschema.NewDeserializer(serializer.client, serde.ValueSerde, jsonDeserializerConfig)

	if err != nil {
		return nil, errors.Join(err, serializer.Close())
	}

	return serializer, nil
}
