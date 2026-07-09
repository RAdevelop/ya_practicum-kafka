package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
)

func main() {

	var cfg config.Config
	cfg.Load(".env")

	fmt.Printf("%+v\n", cfg.Emitter)
	fmt.Printf("%+v\n", cfg.Processor)
	fmt.Printf("%+v\n", cfg.Topic)

	ctx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	var wg sync.WaitGroup

	// 1. запускаем эмиттер для загрузки и обновления запрещенных слов
	wg.Add(1)
	go func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		//TODO создать эмиттер
		//TODO вызвать defer с закрытием эмиттера ("эмиттер.Finish()")
		for {
			select {
			case <-ctx.Done():
				fmt.Println("emitter for bad words has been stopped")
				return
			//default:
			case <-time.After(time.Second): //TODO del
				fmt.Println("TODO отправить сообщение с новым запрещенным словом")
			}
		}
	}(ctx, &wg)
	// 2. запускаем эмиттер для генерации сообщений
	wg.Add(1)
	go func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		//TODO создать эмиттер
		//TODO вызвать defer с закрытием эмиттера ("эмиттер.Finish()")
		for {
			select {
			case <-ctx.Done():
				fmt.Println("emitter for message generate has been stopped")
				return
			//default:
			case <-time.After(time.Second): //TODO del

				fmt.Println("TODO отправить сообщение")
			}
		}
	}(ctx, &wg)
	// 3. запускаем эмиттер для загрузки обновления кто кого заблокировал
	// 4. запускаем процессор для проверки возможности отправки отдельно взятого сообщения между пользователями
	// 5. запускаем процессор для выполнения задачи цензуры над сообщением
	// 6. запускаем процессор для вывода на экран, какое кому сообщение "будет" отправлено

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait
	cancelApp()

	wg.Wait()
}
