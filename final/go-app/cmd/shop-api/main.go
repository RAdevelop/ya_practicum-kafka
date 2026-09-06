package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/emitter"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/processor"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/models"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/serializer"
)

func main() {

	ctx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	appLogger := logger.New("[AppLogger]")
	var cfg config.Config
	cfg.Load(".env")

	serialize, err := serializer.NewJson[models.Product](cfg)
	if err != nil {
		appLogger.Error("Failed to create json serializer , error: %v", err)
		return
	}
	defer func() {
		err = serialize.Close()
		if err != nil {
			appLogger.Error("Failed to close serializer , error: %v", err)
		}
	}()
	codecProducts := jsCodec.NewJsonCodec[models.Product](cfg.Topics.Products, serialize)

	// создаем View таблицу для возможности получать данные из постоянного хранилища запрещенных товаров
	/*
		BlockedProductsViewLogger := logger.New("[BlockedProductsView]")
		BlockedProductsView, err := view.NewView(ctx, codecProducts, cfg, BlockedProductsViewLogger)
		if err != nil {
			BlockedProductsViewLogger.Error("Failed to create view: %v", err)
			return
		}
	*/
	productsEmitter, err := emitter.NewProducts(cfg, codecProducts)

	if err != nil {
		appLogger.Error("Failed to create productsEmitter, error: %v", err)
		return
	}
	defer func() {
		err := productsEmitter.Finish()
		if err != nil {
			appLogger.Error("Failed to close emitter, error: %v", err)
		}
	}()

	// создаем эмиттер для добавления запрещенных товаров
	blockedProductsEmitter, err := emitter.NewProductsBlocked(cfg, codecProducts)
	if err != nil {
		appLogger.Error("Failed to create BlockedProductsEmitter: %v", err)
		return
	}
	defer func() {
		err = blockedProductsEmitter.Finish()
		if err != nil {
			appLogger.Error("Failed to finish BlockedProducts Emitter %v", err)
		}
	}()

	// Читаем товары из файла
	products, err := loadProducts("data/shop-products.json")
	if err != nil {
		appLogger.Error("Failed to load products, error: %v", err)
		return
	}

	appLogger.Info("Loaded products, count: %d", len(products))

	// Публикуем товары
	emitProducts(appLogger, products, productsEmitter)

	var wg sync.WaitGroup

	wg.Add(1)
	processorProductsBlocked(ctx, cfg, &wg)

	go func() {
		wait := make(chan os.Signal, 1)
		signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
		<-wait
		log.Println("Received shutdown signal, cancelling context...")
		cancelApp()
	}()

	wg.Wait()
}

func emitProducts(logger *logger.Logger, products []models.Product, productsEmitter *emitter.Products) {
	for _, product := range products {
		key := product.ProductId
		if err := productsEmitter.EmitSync(key, product); err != nil {
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

func processorProductsBlocked(ctx context.Context, cfg config.Config, wg *sync.WaitGroup) {
	defer wg.Done()

	processor.NewProductsBlocked(cfg).Run(ctx)
}
