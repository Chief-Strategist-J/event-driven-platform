package routers

import (
	"database/sql"
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
	"event-driven-platform/src/features/encryption"
)

func RegisterEncryptionRoutes(mux *http.ServeMux, db *sql.DB) {
	svc := encryption.NewService(db)
	h := handlers.NewEncryptionHandler(svc)

	mux.HandleFunc("POST /api/v1/encryption/encrypt", h.HandleEncryptField)
	mux.HandleFunc("POST /api/v1/encryption/decrypt", h.HandleDecryptField)
	mux.HandleFunc("POST /api/v1/encryption/erasure", h.HandleShredSubject)
	mux.HandleFunc("POST /api/v1/encryption/blind-index", h.HandleGenerateBlindIndex)
}
