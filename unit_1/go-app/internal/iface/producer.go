package iface

type Producer[T any] interface {
	SendMessage(topic string, msg *T) error
	Close()
}
