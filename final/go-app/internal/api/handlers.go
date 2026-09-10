package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/emitter"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/goka/store"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/models"
	"github.com/lovoo/goka"
)

type Emitters struct {
	ProductsEmitter        *emitter.Products
	BlockedProductsEmitter *emitter.ProductsBlocked
}
type Views struct {
	BlockedProductsView         *goka.View
	RecommendationsProductsView *goka.View
}
type Handlers struct {
	logger   *logger.Logger
	config   config.Config
	views    *Views
	emitters *Emitters
}

func NewHandlers(config config.Config, views *Views, emitters *Emitters) *Handlers {
	return &Handlers{
		logger: logger.New("[API]"),
		config: config,
		views:  views,
		//senderView:   senderView,
		emitters: emitters,
	}
}

func (h *Handlers) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.logger.Error("Failed to encode JSON: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// GetShopProductsBlocked - GET /shop/products/blocked — список запрещенных слов
func (h *Handlers) GetShopProductsBlocked(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	val, err := h.views.BlockedProductsView.Get(h.config.KeyTopic.ProductsBlocked)
	if err != nil {
		h.logger.Error("Failed to get blocked products: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	var productsBlockedStore *store.ProductsBlockedStore
	productsBlockedStore, ok := val.(*store.ProductsBlockedStore)
	if ok {
		h.writeJSON(w, http.StatusOK, productsBlockedStore)
		return
	}

	h.logger.Error("wrong type: %T", val)
	http.Error(w, "ProductsBlocked list is empty", http.StatusBadRequest)
}

// PostShopProductsBlockedAction - GET /shop/products/blocked/{action}/{productName} - "add|remove" товар с именем {productName}
func (h *Handlers) PostShopProductsBlockedAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	action := r.PathValue("action")
	if action != "add" && action != "remove" {
		http.Error(w, "Action must be: add or remove", http.StatusBadRequest)
		return
	}

	productName := r.PathValue("productName")
	productName = strings.TrimSpace(productName)

	if productName == "" {
		http.Error(w, "productName is empty", http.StatusBadRequest)
		return
	}

	event := action + ":" + productName

	err := h.emitters.BlockedProductsEmitter.EmitSync(h.config.KeyTopic.ProductsBlocked, event)
	if err != nil {
		h.logger.Error("Failed to emit block state for event: %s, err: %v", event, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	h.logger.Success("EmitSync event: %s", event)
	h.writeJSON(w, http.StatusCreated, map[string]string{"status": "ok", "event": event})
}

// GetClientSearch - GET /client/search?name=имя_товара — поиск товаров
func (h *Handlers) GetClientSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	productName := r.URL.Query().Get("name")

	//TODO del
	log.Printf("------------------------ productName %s", productName)

	category := "Фото и видео"

	//TODO by product.Category

	val, err := h.views.RecommendationsProductsView.Get(category)
	if err != nil {
		h.logger.Error("Failed to get Recommendations: %v", err)
	}

	searchResult := models.SearchResult{}

	var recommendations *store.Recommendations
	recommendations, ok := val.(*store.Recommendations)
	if ok {
		searchResult.Recommendations = recommendations
	} else {
		h.logger.Error("wrong type for Recommendations: %T", val)
	}

	h.writeJSON(w, http.StatusOK, searchResult)
}
