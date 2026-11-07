package middleware

import (
	"net/http"
	"os"
	"strconv"
)

const DefaultMaxRequestBodyBytes int64 = 1 << 20 // 1 MiB

// MaxRequestBodyBytesFromEnv reads MAX_REQUEST_BODY_BYTES, falling back to 1 MiB.
func MaxRequestBodyBytesFromEnv() int64 {
	if v := os.Getenv("MAX_REQUEST_BODY_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return DefaultMaxRequestBodyBytes
}

// BodyLimitMiddleware rejects requests whose declared Content-Length exceeds max with
// 413 and the standard error envelope, and caps undeclared/chunked bodies with
// http.MaxBytesReader (reads beyond max fail, so handlers answer with a decode error).
func BodyLimitMiddleware(max int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > max {
				writeErrorEnvelope(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body exceeds the maximum allowed size", r)
				return
			}
			if r.Body != nil && r.Body != http.NoBody {
				r.Body = http.MaxBytesReader(w, r.Body, max)
			}
			next.ServeHTTP(w, r)
		})
	}
}
