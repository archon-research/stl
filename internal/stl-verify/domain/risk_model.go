package domain

type RiskModel struct {
	ID    string
	Score float64
}

func CalculateRisk(data interface{}) float64 {
	return 0.5
}
