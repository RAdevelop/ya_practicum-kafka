package model

import "strconv"

// Message - представление отдельно взятого сообщения (кто кому отправил)
type Message struct {
	ID         int64  `json:"id"`
	FromUserID int64  `json:"from_user_id"`
	ToUserID   int64  `json:"to_user_id"`
	Text       string `json:"text"`
}

func (m Message) IDToString() string {
	return strconv.FormatInt(m.ID, 10)
}
