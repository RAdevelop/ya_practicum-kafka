package store

import "strings"

type ProductsBlockedStore struct {
	Products map[string]struct{} `json:"products"`
}

// Add - добавляем товар в список заблокированных
func (p *ProductsBlockedStore) Add(productName string) {

	productName = strings.TrimSpace(productName)

	if productName == "" {
		return
	}

	productName = strings.ToLower(productName)

	if p.Products == nil {
		p.Products = make(map[string]struct{})
	}

	if _, exists := p.Products[productName]; exists {
		return
	}

	p.Products[productName] = struct{}{}
}

func (p *ProductsBlockedStore) Remove(productName string) {
	productName = strings.TrimSpace(productName)
	if productName == "" {
		return
	}
	productName = strings.ToLower(productName)
	if p.Products == nil {
		return
	}
	if _, exists := p.Products[productName]; exists {
		delete(p.Products, productName)
	}
}

func (p *ProductsBlockedStore) IsBlocked(productName string) bool {
	if p == nil || p.Products == nil {
		return false
	}
	_, exists := p.Products[strings.ToLower(strings.TrimSpace(productName))]
	return exists
}
