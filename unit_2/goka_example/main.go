package main

import (
	"context"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers                 = []string{"192.168.50.128:19094", "192.168.50.128:29094", "192.168.50.128:39094"} // Адрес брокера
	topicInput  goka.Stream = "input"                                                                          // Топик с исходными данными
	topicOutput goka.Stream = "output"                                                                         // Топик с результатом
	topicGroup  goka.Group  = "upper-case-group"                                                               // Название группы процессора
)

type gokaCodec interface {
	codec.Int64 | codec.String | codec.Bytes
}

// runEmitter - Генерирует сообщения (строки) и отправляет их в топик topicInput каждую секунду
func runEmitter(ctx context.Context, codec goka.Codec) {
	emitter, err := goka.NewEmitter(brokers, topicInput, codec)
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer func() {
		err := emitter.Finish()
		if err != nil {
			log.Printf("error closing emitter: %v", err)
		}
	}()

	var counter int
	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down emitter")
			return
		case <-time.After(1 * time.Second):
			//err = emitter.EmitSync("key", fmt.Sprintf("Value #%d", counter))
			err = emitter.EmitSync("key", rand.Int64N(1_000_000))
			if err != nil {
				log.Fatalf("error emitting message: %v", err)
			}
			log.Printf("[emitter] Сообщение #%d отправлено\n", counter)
			counter++
		}
	}
}

// upperCaseFunc - Обработчик — преобразует значения сообщений в upper-case и отправляет их в output
func upperCaseFunc(ctx goka.Context, msg interface{}) {
	log.Printf("[processor] Получено сообщение: key = %s, value = %s", ctx.Key(), msg)

	if strMsg, ok := msg.(string); ok {
		upper := strings.ToUpper(strMsg)

		// Отправляем сообщения в output
		ctx.Emit(topicOutput, ctx.Key(), upper)
		log.Printf("[processor] Сообщение обработано: key = %s, new_value = %s", ctx.Key(), upper)
	}
}

// upperCaseFunc - Обработчик — преобразует значения сообщений в upper-case и отправляет их в output
func square(ctx goka.Context, msg interface{}) {
	log.Printf("[processor] Получено сообщение: key = %s, value = %d", ctx.Key(), msg)

	if int64Msg, ok := msg.(int64); ok {
		msgSquare := int64Msg * int64Msg

		// Отправляем сообщения в output
		ctx.Emit(topicOutput, ctx.Key(), msgSquare)
		log.Printf("[processor] Сообщение обработано: key = %s, new_value = %d", ctx.Key(), msgSquare)
	}
}

func defineGroup(codec goka.Codec, processFunc func(ctx goka.Context, msg interface{})) *goka.GroupGraph {
	return goka.DefineGroup(topicGroup,
		goka.Input(topicInput, codec, processFunc),
		goka.Output(topicOutput, codec),
	)
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var gCodec goka.Codec = new(codec.Int64)

	go runEmitter(ctx, gCodec) // Запуск эмиттера в отдельной горутине

	// Создание группы
	//g := defineGroup(gCodec, upperCaseFunc)
	g := defineGroup(gCodec, square)

	// Создание процессора
	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}

	// Запуск процессора в отдельной горутине

	done := make(chan bool)
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			log.Fatalf("error running processor: %v", err)
		}

		log.Printf("Processor shutdown cleanly")
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait
	cancel()
	<-done
}
