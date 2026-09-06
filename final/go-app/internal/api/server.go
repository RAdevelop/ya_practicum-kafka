package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/logger"
)

type Server struct {
	httpServer *http.Server
	handlers   *Handlers
	logger     *logger.Logger
}

func NewServer(handlers *Handlers) *Server {
	mux := http.NewServeMux()

	// Регистрируем эндпоинты

	// выводит список заблокированных товаров из постоянного хранилища
	mux.HandleFunc("GET /shop/products/blocked", handlers.GetShopProductsBlocked)
	// "add|remove" товар с именем {productName}
	mux.HandleFunc("GET /shop/products/blocked/{action}/{productName}", handlers.PostShopProductsBlockedAction)
	// PostBadWord - имя оставим такое, хоть и GET запрос (чтобы не реализовывать html форму)
	//mux.HandleFunc("GET /bad-word", handlers.PostBadWord)
	// состояние блокировки пользователей для указанного
	//mux.HandleFunc("GET /user-block/{user_id}", handlers.GetUserBlock)

	// PostMessage
	//mux.HandleFunc("GET /message/{from_uid}/{to_uid}/", handlers.PostMessage)

	return &Server{
		httpServer: &http.Server{
			Addr:         ":8181",
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
		handlers: handlers,
		logger:   logger.New("[APIServer]"),
	}
}

func (s *Server) Run(ctx context.Context) error {
	s.logger.Info("Starting HTTP server on %s", "http://localhost:8181")
	s.logger.Info("GET /shop/products/blocked: %s", "http://localhost:8181/shop/products/blocked")
	s.logger.Info("GET /shop/products/blocked/{action}/{productName}: %s", "http://localhost:8181/shop/products/blocked/{action}/{productName}")

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	s.logger.Info("Shutting down HTTP server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.httpServer.Shutdown(shutdownCtx)
}
