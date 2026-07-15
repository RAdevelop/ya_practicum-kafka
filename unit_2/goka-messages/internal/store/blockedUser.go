package store

// BlockedUsersStore - представление списка заблокированных пользователей (BlockedUserIDs) для указанного пользователя (UserID)
type BlockedUsersStore struct {
	UserID string `json:"user_id"`
	// Определяем карту, чтобы потом проще было искать по ключу в карте
	BlockedUserIDs map[string]bool `json:"blocked_user_ids"`
}

func NewBlockedUsersStore(userID string) *BlockedUsersStore {
	return &BlockedUsersStore{
		UserID:         userID,
		BlockedUserIDs: make(map[string]bool),
	}
}

// Block - блокируем пользователя blockUserID
func (bus *BlockedUsersStore) Block(blockUserID string) {
	if bus.BlockedUserIDs == nil {
		bus.BlockedUserIDs = make(map[string]bool)
	}

	bus.BlockedUserIDs[blockUserID] = true
}

// Unblock - разблокируем пользователя unblockUserID
func (bus *BlockedUsersStore) Unblock(unblockUserID string) {
	if bus.BlockedUserIDs == nil {
		return
	}

	if _, exists := bus.BlockedUserIDs[unblockUserID]; exists {
		delete(bus.BlockedUserIDs, unblockUserID)
	}
}
