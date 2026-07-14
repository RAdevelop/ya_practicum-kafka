package store

import "strings"

type BadWordsStore struct {
	Words map[string]string `json:"words"`
}

// AddWord - добавляем новое слово в список запрещенных
func (s *BadWordsStore) AddWord(word string) {

	word = strings.TrimSpace(word)

	if word == "" {
		return
	}

	word = strings.ToLower(word)

	if s.Words == nil {
		s.Words = make(map[string]string)
	}

	if _, exists := s.Words[word]; exists {
		return
	}

	s.Words[word] = (func(word string) string {
		return strings.Repeat("*", len([]rune(word)))
	})(word)
}

// GetMask - получаем маску для указанного слова
func (s *BadWordsStore) GetMask(word string) (string, bool) {
	if s.Words == nil {
		return "", false
	}
	word = strings.TrimSpace(word)
	if word == "" {
		return "", false
	}
	word = strings.ToLower(word)
	if mask, exists := s.Words[word]; exists {
		return mask, exists
	}
	return "", false
}
