package main

import (
	"context"
	"log"
	"math/rand/v2"
	"strconv"
	"time"

	jsonCodec "github.com/RAdevelop/ya_practicum-kafka/unit_2/go-app/goka_example/codec"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

/*
Предварительно создать топики:

docker exec -it kafka-b-1 kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 --config min.insync.replicas=2

docker exec -it kafka-b-1 kafka-topics --create --topic users.sum --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 --config min.insync.replicas=2

docker exec -it kafka-b-1 kafka-topics --create --topic users.category --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 --config min.insync.replicas=2

docker exec -it kafka-b-1 kafka-topics --create --topic users-sum-group-table --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 --config min.insync.replicas=2
*/
var (
	brokers = []string{"192.168.50.128:19094", "192.168.50.128:29094", "192.168.50.128:39094"}

	topicOrders        goka.Stream = "orders"
	topicUsersSum      goka.Stream = "users.sum"
	topicUsersCategory goka.Stream = "users.category"

	groupUsersOrderSum goka.Group = "users-sum-group"
	groupUsersCategory goka.Group = "users-category-group"
	groupLogger        goka.Group = "logger-group"
)

// Order описывает сообщение в топике с заказами
type Order struct {
	UserID      int64 `json:"user_id"`
	OrderID     int64 `json:"order_id"`
	OrderAmount int64 `json:"order_amount"`
}

// UserSum сумма всех заказов пользователя
type UserSum struct {
	Total int64 `json:"total"`
}

// UserCategory категория пользователя
type UserCategory struct {
	Category string `json:"category"`
}

func main() {
	go purchasesEmitter()
	go sumProcessor()
	go categoryProcessor()
	go loggerProcessor()

	select {} // Блокируем main, чтобы горутины работали
}

// purchasesEmitter — эмиттер, который генерирует данные в топик purchases
func purchasesEmitter() {

	jc := jsonCodec.NewJsonCodec[Order]()

	e, err := goka.NewEmitter(brokers, topicOrders, jc)
	if err != nil {
		log.Fatal(err)
	}

	defer e.Finish()

	for {
		time.Sleep(1 * time.Second)

		up := Order{
			UserID:      rand.Int64N(10),            // Случайный идентификатор пользователя в диапазоне [0, 10)
			OrderID:     rand.Int64(),               // Случайный идентификатор покупки
			OrderAmount: 1000 + rand.Int64N(90_000), // Случайный сумма покупки в диапазоне [1_000, 100_000)
		}
		emitKey := strconv.FormatInt(up.UserID, 10)
		if err = e.EmitSync(emitKey, up); err != nil {
			log.Fatal(err)
		}
		log.Printf("(emitKey=%s) Новая покупка пользователя %d на сумму %d\n", emitKey, up.UserID, up.OrderAmount)
	}
}

// sumProcessor считает сумму всех заказов
func sumProcessor() {
	processFunc := func(ctx goka.Context, msg interface{}) {
		var (
			order Order
			ok    bool
		)
		if order, ok = msg.(Order); !ok {
			log.Printf("illegal type: %T", msg)
			return // Не останавливаем процессор
		}

		// Считываем текущее значение для пользователя
		var userSum UserSum
		currentSum := ctx.Value() // Значение для ключа сообщения — а оно совпадает с идентификатором пользователя
		log.Printf("------------ ctx.Value() = %v, ctx.Key() = %v\n", currentSum, ctx.Key())
		if currentSum != nil {
			userSum = currentSum.(UserSum)
		}

		// И добавляем сумму к этому пользователю
		userSum.Total += order.OrderAmount
		ctx.SetValue(userSum)
		log.Printf("Текущая сумма заказов пользователя %s: %d\n", ctx.Key(), userSum.Total)

		// Отправляем обновленную сумму в следующий топик
		ctx.Emit(topicUsersSum, ctx.Key(), userSum)
	}

	g := goka.DefineGroup(groupUsersOrderSum,
		goka.Input(topicOrders, new(jsonCodec.JsonCodec[Order]), processFunc),
		goka.Persist(new(jsonCodec.JsonCodec[UserSum])),
		goka.Output(topicUsersSum, new(jsonCodec.JsonCodec[UserSum])),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Stop()

	if err = p.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// categoryProcessor присваивает категорию пользователю
func categoryProcessor() {
	processFunc := func(ctx goka.Context, msg interface{}) {
		var (
			userSum UserSum
			ok      bool
		)
		if userSum, ok = msg.(UserSum); !ok {
			log.Printf("illegal type: %T", msg)
			return // Не останавливаем процессор
		}

		var category string
		switch {
		case userSum.Total >= 1_000_000: // Если сумма покупок больше 1_000_000, то категория gold
			category = "gold"
		case userSum.Total >= 500_000: // Если сумма заказов больше 500_000, то категория silver
			category = "silver"
		default: // Иначе — категория bronze
			category = "bronze"
		}

		currentCategory := ctx.Value()
		// Либо это первый заказ пользователя, либо у него изменилась категория
		if currentCategory == nil || currentCategory.(string) != category {
			// Сохраняем текущую категорию в групповую таблицу
			ctx.SetValue(category)
			// И отправляем сообщение в топик
			userCategory := UserCategory{Category: category}
			log.Printf("[categoryProcessor] -- Для пользователя %s новая категория = %s, UserCategory = %v\n\n", ctx.Key(), category, userCategory)
			ctx.Emit(topicUsersCategory, ctx.Key(), userCategory)
		}
	}

	g := goka.DefineGroup(groupUsersCategory,
		goka.Input(topicUsersSum, new(jsonCodec.JsonCodec[UserSum]), processFunc),
		goka.Persist(new(codec.String)),
		goka.Output(topicUsersCategory, new(jsonCodec.JsonCodec[UserCategory])),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	if err = p.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// loggerProcessor просто логирует финальную категорию пользователя
func loggerProcessor() {
	processFunc := func(ctx goka.Context, msg interface{}) {
		if userCategory, ok := msg.(UserCategory); ok {
			log.Printf("[loggerProcessor] -- Категория пользователя = %s\n\n", userCategory.Category)
		}
	}

	g := goka.DefineGroup(groupLogger,
		goka.Input(topicUsersCategory, new(jsonCodec.JsonCodec[UserCategory]), processFunc),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	if err = p.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
