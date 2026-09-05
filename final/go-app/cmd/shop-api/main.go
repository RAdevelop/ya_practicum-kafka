package main

import (
	"encoding/json"
	"os"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	jsCodec "github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/codec"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/emitter"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/models"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/serializer"
)

func main() {

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

	productsEmitter, err := emitter.NewProducts(cfg, codecProducts)

	if err != nil {
		appLogger.Error("Failed to create emitter, error: %v", err)
	}
	defer func() {
		err := productsEmitter.Finish()
		if err != nil {
			appLogger.Error("Failed to close emitter, error: %v", err)
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
