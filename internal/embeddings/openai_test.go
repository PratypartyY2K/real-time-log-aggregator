package embeddings

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenAIClientEmbedsBatchInResponseIndexOrder(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatal("missing bearer token")
		}
		fmt.Fprint(w, `{"data":[{"index":1,"embedding":[0,1]},{"index":0,"embedding":[1,0]}]}`)
	}))
	defer server.Close()

	vectors, err := (OpenAIClient{APIKey: "test-key", Model: "text-embedding-3-small", Dimensions: 2, URL: server.URL}).Embed(context.Background(), []string{"a", "b"})
	if err != nil || vectors[0][0] != 1 || vectors[1][1] != 1 {
		t.Fatalf("vectors=%v err=%v", vectors, err)
	}
}
