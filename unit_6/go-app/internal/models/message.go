package models

import (
	"fmt"
	"strconv"
)

// Message - структура отправляемых/получаемых сообщений
type Message struct {
	ID    int64  `json:"id" avro:"id"`
	MType string `json:"type" avro:"type"`
	Delta *int64 `json:"delta" avro:"delta"`
}

// String - простое строковое представление Message
func (m *Message) String() string {

	var delta string
	delta = "null"
	if m.Delta != nil {
		delta = strconv.FormatInt(*m.Delta, 10)
	}

	return fmt.Sprintf("Message{ID:%d, MType:%q, Delta:%s}", m.ID, m.MType, delta)
}

func (m *Message) IdAsByte() []byte {
	return []byte(fmt.Sprintf("%d", m.ID))
}
