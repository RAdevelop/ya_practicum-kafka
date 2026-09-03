package models

import "time"

type Product struct {
	ProductId      string         `json:"product_id"`
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	Price          Price          `json:"price"`
	Category       string         `json:"category"`
	Brand          string         `json:"brand"`
	Stock          Stock          `json:"stock"`
	Sku            string         `json:"sku"`
	Tags           []string       `json:"tags"`
	Images         []Image        `json:"images"`
	Specifications Specifications `json:"specifications"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	Index          string         `json:"index"`
	StoreId        string         `json:"store_id"`
}

type Price struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

type Stock struct {
	Available int `json:"available"`
	Reserved  int `json:"reserved"`
}

type Image struct {
	Url string `json:"url"`
	Alt string `json:"alt"`
}

type Specifications struct {
	Weight          string `json:"weight"`
	Dimensions      string `json:"dimensions"`
	BatteryLife     string `json:"battery_life"`
	WaterResistance string `json:"water_resistance"`
}
