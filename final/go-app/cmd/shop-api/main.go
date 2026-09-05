package main

import (
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
		appLogger.Error("Failed to create json serializer", "error", err)
		return
	}
	defer func() {
		err = serialize.Close()
		if err != nil {
			appLogger.Error("Failed to close serializer", "error", err)
		}
	}()

	codecProducts := jsCodec.NewJsonCodec[models.Product](cfg.Topics.Products, serialize)

	productsEmitter, err := emitter.NewProducts(cfg, codecProducts)

	if err != nil {
		appLogger.Error("Failed to create emitter", "error", err)
	}
	defer func() {
		err := productsEmitter.Finish()
		if err != nil {
			appLogger.Error("Failed to close emitter", "error", err)
		}
	}()
}
