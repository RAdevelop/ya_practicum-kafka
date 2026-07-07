package main

import (
	"log"

	codecForGoka "github.com/RAdevelop/ya_practicum-kafka/unit_2/go-app/goka_example/codec"
	"github.com/lovoo/goka"
)

func main() {
	/*
			предварительно:

		   docker exec -it kafka-b-1 kafka-topics --create --topic users --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 --config min.insync.replicas=2

	*/
	var usersTopic goka.Stream = "users"

	// Создайте эмиттер, который отправляет user в топик users.
	var brokers = []string{"192.168.50.128:19094", "192.168.50.128:29094", "192.168.50.128:39094"}
	emitter, err := goka.NewEmitter(brokers, usersTopic, new(codecForGoka.JsonCodec[User]))
	if err != nil {
		log.Fatal(err)
	}
	defer emitter.Finish() // Остановка эмиттера, то есть закрытие соединения с брокером

	user := User{Name: "Test user"}
	err = emitter.EmitSync("key", user)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[emitter] Сообщение %v отправлено\n", user)
}

type User struct {
	Name string `json:"name"`
}
