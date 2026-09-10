package helper

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/RAdevelop/ya_practicum-kafka/final/go-app/internal/models"
)

// LoadProductsFromJSONL - загружает товары из JSONL-файла
func LoadProductsFromJSONL(filePath string) ([]models.Product, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var products []models.Product
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" { // пропускаем пустые строки
			continue
		}

		var p models.Product
		if err = json.Unmarshal([]byte(line), &p); err != nil {
			return nil, err
		}
		products = append(products, p)
	}

	if scanner.Err() != nil {
		return nil, scanner.Err()
	}

	return products, nil
}

func LoadProductsFromJSON(filePath string) ([]models.Product, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var products []models.Product
	if err = json.Unmarshal(data, &products); err != nil {
		return nil, err
	}

	return products, nil
}

func RemoveInPlace[T any](slice []T, cond func(T) bool) []T {
	write := 0
	for read := 0; read < len(slice); read++ {
		if cond(slice[read]) {
			slice[write] = slice[read]
			write++
		}
	}
	return slice[:write]
}
