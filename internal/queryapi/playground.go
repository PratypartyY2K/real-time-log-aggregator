package queryapi

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed playground/*
var playgroundFiles embed.FS

func PlaygroundHandler() http.Handler {
	content, err := fs.Sub(playgroundFiles, "playground")
	if err != nil {
		panic(err)
	}
	return http.StripPrefix("/playground/", http.FileServer(http.FS(content)))
}

func OpenAPIHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		payload, err := playgroundFiles.ReadFile("playground/openapi.yaml")
		if err != nil {
			http.Error(w, "OpenAPI contract unavailable", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(payload)
	})
}
