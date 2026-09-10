package models

import "time"

type ClientSearch struct {
	QueryId    string    `json:"query_id"`
	UserId     string    `json:"user_id"`
	SearchTerm string    `json:"search_term"`
	Timestamp  time.Time `json:"timestamp"`
	Source     string    `json:"source"`
}
