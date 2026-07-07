package codec

import (
	"encoding/json"
	"fmt"
)

func NewJsonCodec[T any]() *JsonCodec[T] {
	return &JsonCodec[T]{}
}

type JsonCodec[T any] struct {
}

func (jc *JsonCodec[T]) Encode(value interface{}) ([]byte, error) {
	if v, ok := value.(T); ok {
		return json.Marshal(v)

	}
	return nil, fmt.Errorf("illegal type: %T", value)
}

func (jc *JsonCodec[T]) Decode(data []byte) (interface{}, error) {
	var v T
	err := json.Unmarshal(data, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}
