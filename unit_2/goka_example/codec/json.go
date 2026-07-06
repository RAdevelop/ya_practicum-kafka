package codec

import "encoding/json"

func NewJsonCodec[T any]() *JsonCodec[T] {
	return &JsonCodec[T]{}
}

type JsonCodec[T any] struct {
}

func (jc *JsonCodec[T]) Encode(value interface{}) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (jc *JsonCodec[T]) Decode(data []byte) (interface{}, error) {
	var v T
	err := json.Unmarshal(data, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}
