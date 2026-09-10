package models

import "github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/store"

type SearchResult struct {
	Product         *Product               `json:"product"`
	Recommendations *store.Recommendations `json:"recommendations"`
}
