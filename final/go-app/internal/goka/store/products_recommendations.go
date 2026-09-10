package store

type RecommendedProduct struct {
	ProductId string `json:"product_id"`
	Name      string `json:"name"`
	Brand     string `json:"brand"`
}

type Recommendations struct {
	Category            string               `json:"category"`
	RecommendedProducts []RecommendedProduct `json:"recommended_products"`
}
