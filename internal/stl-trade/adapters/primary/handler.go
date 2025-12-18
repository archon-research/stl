package primary

import "net/http"

func HandleTrade(w http.ResponseWriter, r *http.Request) {
	// Parse request and call service
	w.WriteHeader(http.StatusOK)
}
