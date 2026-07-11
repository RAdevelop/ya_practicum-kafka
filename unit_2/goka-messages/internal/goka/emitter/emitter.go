package emitter

import (
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	"github.com/lovoo/goka"
)

type Emitter struct {
	emitter *goka.Emitter
}

func NewEmitter(topic goka.Stream, config config.Config, codec goka.Codec, options ...goka.EmitterOption) (*Emitter, error) {
	emitter, err := goka.NewEmitter(config.Brokers, topic, codec, options...)
	if err != nil {
		return nil, err
	}

	return &Emitter{
		emitter: emitter,
	}, nil
}

func (em *Emitter) Finish() error {
	return em.emitter.Finish()
}

func (em *Emitter) EmitSync(key string, msg interface{}) error {
	return em.emitter.EmitSync(key, msg)
}
