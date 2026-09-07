package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/api"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/emitter"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/processor"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/store"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/view"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/models"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/serializer"
	"github.com/lovoo/goka/codec"
)

func main() {
	ctx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	appLogger := logger.New("[AppLogger]")
	var cfg config.Config
	cfg.Load(".env")

	// 1. Инициализация кодека
	serialize, err := serializer.NewJson[models.Product](cfg)
	if err != nil {
		appLogger.Error("Failed to create json serializer: %v", err)
		return
	}
	defer func() {
		if err := serialize.Close(); err != nil {
			appLogger.Error("Failed to close serializer: %v", err)
		}
	}()
	codecProducts := jsCodec.NewJsonCodec[models.Product](cfg.Topics.Products, serialize)

	// 2. Создание эмиттеров
	// эмиттер отправка не фильтрованных товаров в Кафка
	productsEmitter, err := emitter.NewProducts(cfg, codecProducts)
	if err != nil {
		appLogger.Error("Failed to create productsEmitter: %v", err)
		return
	}
	defer func() {
		if err := productsEmitter.Finish(); err != nil {
			appLogger.Error("Failed to close productsEmitter: %v", err)
		}
	}()

	// эмиттер блокировки товаров по имени
	blockedProductsEmitter, err := emitter.NewProductsBlocked(cfg, new(codec.String))
	if err != nil {
		appLogger.Error("Failed to create BlockedProductsEmitter: %v", err)
		return
	}
	defer func() {
		if err := blockedProductsEmitter.Finish(); err != nil {
			appLogger.Error("Failed to finish BlockedProductsEmitter: %v", err)
		}
	}()

	emitters := &api.Emitters{
		ProductsEmitter:        productsEmitter,
		BlockedProductsEmitter: blockedProductsEmitter,
	}

	// 3. Запуск процессоров с сигналами готовности
	var wg sync.WaitGroup

	// 3.1. Запускаем ProductsBlocked (создаёт топик group-products-blocked-table)
	blockedProcessorReady := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		pb := processor.NewProductsBlocked(cfg, blockedProcessorReady)
		pb.Run(ctx)
	}()

	// Ждём, пока ProductsBlocked создаст топик
	select {
	case <-blockedProcessorReady:
		appLogger.Info("ProductsBlocked processor is ready (topic created)")
	case <-time.After(30 * time.Second):
		appLogger.Error("Timeout waiting for ProductsBlocked processor")
		return
	case <-ctx.Done():
		appLogger.Error("context cancelled while waiting for ProductsBlocked")
		return
	}

	// 4. Создание View (теперь топик существует)
	blockedProductsViewLogger := logger.New("[BlockedProductsView]")
	blockedProductsView, err := view.NewView(ctx, jsCodec.NewEncodingJson[*store.ProductsBlockedStore](), cfg, blockedProductsViewLogger)
	if err != nil {
		blockedProductsViewLogger.Error("Failed to create view: %v", err)
		return
	}

	// Ждём, пока View загрузит данные
	select {
	case <-blockedProductsView.WaitRunning():
		appLogger.Info("BlockedProductsView is ready")
	case <-time.After(30 * time.Second):
		appLogger.Error("Timeout waiting for BlockedProductsView")
		return
	case <-ctx.Done():
		appLogger.Error("Context cancelled while waiting for BlockedProductsView")
		return
	}

	views := &api.Views{
		BlockedProductsView: blockedProductsView,
	}

	// 5. Запуск цензора (зависит от View)
	censorReady := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		pc := processor.NewProductsCensor(cfg, views, codecProducts, censorReady)
		pc.Run(ctx)
	}()

	// Ждём, пока цензор будет готов (опционально)
	select {
	case <-censorReady:
		appLogger.Info("ProductsCensor processor is ready")
	case <-time.After(30 * time.Second):
		appLogger.Error("Timeout waiting for ProductsCensor processor")
		return
	case <-ctx.Done():
		appLogger.Error("Context cancelled while waiting for ProductsCensor")
		return
	}

	// 6. Запуск HTTP-сервера
	handlers := api.NewHandlers(cfg, views, emitters)
	server := api.NewServer(handlers)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Run(ctx); err != nil {
			appLogger.Error("Server error: %v", err)
		}
	}()

	// 7. Публикация товаров (после готовности всех процессоров)
	products, err := loadProducts("data/shop-products.json")
	if err != nil {
		appLogger.Error("Failed to load products: %v", err)
	}
	appLogger.Info("Loaded products, count: %d", len(products))

	wg.Add(1)
	go emitProducts(&wg, appLogger, products, emitters, cfg)

	// 8. Ожидание сигналов завершения
	go func() {
		wait := make(chan os.Signal, 1)
		signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
		<-wait
		log.Println("Received shutdown signal, cancelling context...")
		cancelApp()
	}()

	wg.Wait()
}

func emitProducts(wg *sync.WaitGroup, logger *logger.Logger, products []models.Product, emitters *api.Emitters, config config.Config) {
	defer wg.Done()

	if len(products) == 0 {
		return
	}

	// добавим для примера в заблокированные товары первый товар из списка:
	event := "add:" + products[0].Name
	err := emitters.BlockedProductsEmitter.EmitSync(config.KeyTopic.ProductsBlocked, event)
	if err != nil {
		logger.Error("Failed to emit block product for event: %s, err: %v", event, err)
	}

	for _, product := range products {
		key := product.ProductId
		if err := emitters.ProductsEmitter.EmitSync(key, product); err != nil {
			logger.Error("Failed to emit product %s: %v, %s: %s", "error", err, "product_id", product.ProductId)
			continue
		}
		logger.Info("Product emitted successfully %s: %s, %s: %s", "product_id", product.ProductId, "name", product.Name)
	}

	logger.Info("All products published, count: %d", len(products))
}

// loadProducts загружает товары из JSON-файла
func loadProducts(filePath string) ([]models.Product, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var products []models.Product
	if err := json.Unmarshal(data, &products); err != nil {
		return nil, err
	}

	return products, nil
}
