package serializer

type Serializable[T any] interface {
	Serialize(topic string, data *T) ([]byte, error)
	Deserialize(topic string, data []byte, result *T) error
	Close() error
}
