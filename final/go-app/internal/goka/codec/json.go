package codec

import (
	"fmt"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/serializer"
)

func NewJsonCodec[T any](topic string, serializer serializer.Serializable[T]) *JsonCodec[T] {

	return &JsonCodec[T]{
		serializer: serializer,
		topic:      topic,
	}
}

type JsonCodec[T any] struct {
	serializer serializer.Serializable[T]
	topic      string
}

func (jc *JsonCodec[T]) Encode(value interface{}) ([]byte, error) {
	if v, ok := value.(T); ok {
		return jc.serializer.Serialize(jc.topic, &v)

	}
	return nil, fmt.Errorf("illegal type: %T", value)
}

func (jc *JsonCodec[T]) Decode(data []byte) (interface{}, error) {
	var v T
	err := jc.serializer.Deserialize(jc.topic, data, &v)

	if err != nil {
		return nil, err
	}
	return v, nil
}
