package serializer

import (
	"errors"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov3"
)

type Avro[T any] struct {
	serialize[T]
}

func NewAvro[T any](config config.Config) (*Avro[T], error) {

	serializer := &Avro[T]{}
	serializer.config = config

	var err error
	serializer.client, err = serializer.schemaRegistryClient()
	if err != nil {
		return nil, errors.Join(err, serializer.Close())
	}

	avroSerializerConfig := avrov3.NewSerializerConfig()
	avroSerializerConfig.AutoRegisterSchemas = false
	avroSerializerConfig.UseLatestVersion = true

	serializer.serializer, err = avrov3.NewSerializer(serializer.client, serde.ValueSerde, avroSerializerConfig)

	if err != nil {
		return nil, errors.Join(err, serializer.Close())
	}

	avroDeserializerConfig := avrov3.NewDeserializerConfig()
	avroDeserializerConfig.UseLatestVersion = true
	avroDeserializerConfig.SubjectNameStrategyType = serde.TopicNameStrategyType

	serializer.deserializer, err = avrov3.NewDeserializer(serializer.client, serde.ValueSerde, avroDeserializerConfig)

	if err != nil {
		return nil, errors.Join(err, serializer.Close())
	}

	return serializer, nil
}
