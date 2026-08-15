package models

// Message - структура отправляемых/получаемых сообщений
type Message struct {
	Before      interface{} `json:"before"`
	After       interface{} `json:"after"`
	Source      interface{} `json:"source"`
	Transaction interface{} `json:"transaction"`
	Op          string      `json:"op"`
	TsMs        int64       `json:"ts_ms"`
	TsUs        int64       `json:"ts_us"`
	TsNs        int64       `json:"ts_ns"`
}
