package models

import "fmt"

// Message - структура отправляемых/получаемых сообщений
type Message struct {
	ID      int    `json:"id"`
	Payload string `json:"payload"`
	Ts      int64  `json:"ts"`
}

// String - простое строковое представление Message
func (m Message) String() string {
	return fmt.Sprintf("Message{ID:%d, Payload:%q, Ts:%d}", m.ID, m.Payload, m.Ts)
}
