package api

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"

	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/goka/emitter"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/model"
	"github.com/RAdevelop/ya_practicum-kafka/unit_2/goka-messages/internal/store"
	"github.com/lovoo/goka"
)

type Emitters struct {
	BadWords  *emitter.Emitter
	Messages  *emitter.Emitter
	BlockUser *emitter.Emitter
}
type Views struct {
	BadWordsView    *goka.View
	BlockedUserView *goka.View
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

// GetBadWords - GET /bad-words — список запрещенных слов
func (h *Handlers) GetBadWords(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	val, err := h.views.BadWordsView.Get(h.config.KeyTopic.BadWords)
	if err != nil {
		h.logger.Error("Failed to get bad words: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	var badWordsStore store.BadWordsStore
	badWordsStore, ok := val.(store.BadWordsStore)
	if ok {
		h.writeJSON(w, http.StatusOK, badWordsStore)
		return
	}

	h.logger.Error("wrong type: %T", val)
	http.Error(w, "bad word list is empty", http.StatusBadRequest)
}

// PostBadWord - POST /bad-word — добавить запрещенное слово
func (h *Handlers) PostBadWord(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()

	badWord := query.Get("word")
	badWord = strings.TrimSpace(badWord)
	if badWord == "" {
		http.Error(w, "Bad word is empty", http.StatusBadRequest)
		return
	}
	err := h.emitters.BadWords.EmitSync(h.config.KeyTopic.BadWords, badWord)
	if err != nil {
		h.logger.Error("Failed to emit bad word: %v", err)
		http.Error(w, "Failed to emit bad word", http.StatusInternalServerError)
		return
	}

	h.logger.Success("EmitSync bad word: %s", badWord)
	h.writeJSON(w, http.StatusCreated, map[string]string{"status": "ok", "badWord": badWord})
}

// GetUserBlock - GET /user-block/{user_id} состояние блокировки пользователей для указанного
func (h *Handlers) GetUserBlock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID := r.PathValue("user_id")

	val, err := h.views.BlockedUserView.Get(userID)
	if err != nil {
		h.logger.Error("Failed to get block state for user_id=%s: %v", userID, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	var storeBlockedUsers *store.BlockedUsersStore
	storeBlockedUsers, ok := val.(*store.BlockedUsersStore)
	if ok {
		h.writeJSON(w, http.StatusOK, storeBlockedUsers)
		return
	}

	h.logger.Error("wrong type: %T", val)
	http.Error(w, "block list is empty", http.StatusBadRequest)
}

// PostUserBlockAction - GET /user-block/{user_id}/{action}/{block_uid} - пользователь {user_id} "block|unblock" пользователя {block_uid}
func (h *Handlers) PostUserBlockAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	action := r.PathValue("action")
	if action != "block" && action != "unblock" {
		http.Error(w, "Action must be: block or unblock", http.StatusBadRequest)
		return
	}

	// проверки на то, что id пользователей это числа и больше нуля пока опустил
	userID := r.PathValue("user_id")
	blockUID := r.PathValue("block_uid")

	if userID == blockUID {
		http.Error(w, "userID is equal blockUID", http.StatusBadRequest)
		return
	}

	event := action + ":" + userID + ":" + blockUID

	err := h.emitters.BlockUser.EmitSync(userID, event)
	if err != nil {
		h.logger.Error("Failed to emit block state for user_id=%s and event=%s, err=%v", userID, event, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	h.logger.Success("EmitSync event: %s", event)
	h.writeJSON(w, http.StatusCreated, map[string]string{"status": "ok", "event": event})
}

// PostMessage - GET message/{from_uid}/{to_uid}/?text=  - отправка сообщения от пользователя from_uid пользователю to_uid
func (h *Handlers) PostMessage(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

	fromUID, err := strconv.ParseUint(r.PathValue("from_uid"), 10, 64)
	if err != nil {
		h.logger.Error("Failed to parse from_uid: %v", err)
		http.Error(w, "from_uid is not int", http.StatusBadRequest)
		return
	}

	toUID, err := strconv.ParseUint(r.PathValue("to_uid"), 10, 64)
	if err != nil {
		h.logger.Error("Failed to parse to_uid: %v", err)
		http.Error(w, "to_uid is not int", http.StatusBadRequest)
		return
	}

	text := strings.TrimSpace(r.URL.Query().Get("text"))
	if text == "" {
		http.Error(w, "text is empty", http.StatusBadRequest)
		return
	}

	mID := rand.Int64()

	message := model.Message{
		ID:         mID,
		FromUserID: fromUID,
		ToUserID:   toUID,
		Text:       text,
	}

	sMessage := message.String()
	if sMessage == "" {
		h.logger.Error("Failed to encode message to string: %v", message)
		http.Error(w, "Failed to encode message to string", http.StatusInternalServerError)
		return
	}

	err = h.emitters.Messages.EmitSync(message.IDToString(), message)
	if err != nil {
		h.logger.Error("Failed to emit message: %v", err)
		http.Error(w, "Failed to emit message", http.StatusInternalServerError)
		return
	}

	h.logger.Success("emit message: %#v", message)
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "message": sMessage})
}
