package codec

import (
	"encoding/json"
	"fmt"
)

func NewEncodingJson[T any]() *EncodingJson[T] {
	return &EncodingJson[T]{}
}

type EncodingJson[T any] struct {
}

func (ej *EncodingJson[T]) Encode(value interface{}) ([]byte, error) {
	if v, ok := value.(T); ok {
		return json.Marshal(v)

	}
	return nil, fmt.Errorf("illegal type: %T", value)
}

func (ej *EncodingJson[T]) Decode(data []byte) (interface{}, error) {
	var v T
	err := json.Unmarshal(data, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}
