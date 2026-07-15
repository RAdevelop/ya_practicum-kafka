package model

import (
	"strconv"

	jsCode "github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/codec"
)

// Message - представление отдельно взятого сообщения (кто кому отправил)
type Message struct {
	ID         int64  `json:"id"`
	FromUserID uint64 `json:"from_user_id"`
	ToUserID   uint64 `json:"to_user_id"`
	Text       string `json:"text"`
}

func (m Message) IDToString() string {
	return strconv.FormatInt(m.ID, 10)
}

func (m Message) String() string {
	codec := jsCode.NewJsonCodec[Message]()
	encodedMessage, err := codec.Encode(m)
	if err != nil {
		return ""
	}
	return string(encodedMessage)
}
