package badword

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/lovoo/goka"
)

func NewEmitter(config config.Config, options ...goka.EmitterOption) (*BadWord, error) {
	brokers := strings.Split(config.Emitter.Brokers, ",")
	emitter, err := goka.NewEmitter(brokers, goka.Stream(config.Topic.BadWords), new(codec.JsonCodec[model.Message]), options...)
	if err != nil {
		return nil, err
	}
	return &BadWord{
		emitter: emitter,
	}, nil
}

type BadWord struct {
	emitter *goka.Emitter
}

func (bw *BadWord) Load(reader io.Reader) error {

	/*
		TODO может добавить слежение за изменениями в файле? такого наблюдателя лучше вынести в отдельный "класс"
	*/
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Printf("Строка: %s\n", line)
	}

	return scanner.Err()
}

func (bw *BadWord) Finish() error {
	return bw.emitter.Finish()
}
