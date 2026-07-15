package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
)

type Server struct {
	httpServer *http.Server
	handlers   *Handlers
	logger     *logger.Logger
}

func NewServer(handlers *Handlers) *Server {
	mux := http.NewServeMux()

	// Регистрируем эндпоинты

	// выводит список запрещенных слов из постоянного хранилища
	mux.HandleFunc("GET /bad-words", handlers.GetBadWords)
	// PostBadWord - имя оставим такое, хоть и GET запрос (чтобы не реализовывать html форму)
	mux.HandleFunc("GET /bad-word", handlers.PostBadWord)
	// состояние блокировки пользователей для указанного
	mux.HandleFunc("GET /user-block/{user_id}", handlers.GetUserBlock)
	// пользователь {user_id} "block|unblock" пользователя {block_uid}
	mux.HandleFunc("GET /user-block/{user_id}/{action}/{block_uid}", handlers.PostUserBlockAction)
	// PostMessage
	mux.HandleFunc("GET /message/{from_uid}/{to_uid}/", handlers.PostMessage)

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
	s.logger.Info("GET /bad-words: %s", "http://localhost:8181/bad-words")
	s.logger.Info("GET /bad-word: %s", "http://localhost:8181/bad-word?word=")
	s.logger.Info("GET /user-block/{user_id}: %s", "http://localhost:8181/user-block/{user_id}")
	s.logger.Info("GET /message/{from_uid}/{to_uid}/: %s", "http://localhost:8181/message/{from_uid}/{to_uid}/")

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
