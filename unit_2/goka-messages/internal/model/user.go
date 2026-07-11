package model

type User struct {
	ID            int64 `json:"id"`
	AcceptBadWord bool  `json:"accept_bad_word"`
}
