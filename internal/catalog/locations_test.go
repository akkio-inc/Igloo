package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/config"
)

// Locations names the location of every table and view: erasure requests
// delete the warehouse's files under none, so a view left out would lose
// its metadata. A table that cannot be loaded is an error, not left out.
func TestLocations(t *testing.T) {
	meta, err := table.NewMetadata(iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32}),
		iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "s3://bucket/warehouse/t", nil)
	if err != nil {
		t.Fatal(err)
	}
	tableMeta, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	brokenTable := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		write := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		p := strings.TrimPrefix(r.URL.Path, "/v1/")
		switch {
		case p == "config":
			write(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{"prefix": "wh"}})
		case p == "wh/namespaces" && r.URL.Query().Get("parent") == "":
			write(map[string]any{"namespaces": [][]string{{"public"}}})
		case p == "wh/namespaces":
			write(map[string]any{"namespaces": [][]string{}})
		case p == "wh/namespaces/public/tables":
			write(map[string]any{"identifiers": []map[string]any{{"namespace": []string{"public"}, "name": "t"},
				{"namespace": []string{"public"}, "name": "dropped"}}})
		case p == "wh/namespaces/public/views":
			write(map[string]any{"identifiers": []map[string]any{{"namespace": []string{"public"}, "name": "v"}}})
		case p == "wh/namespaces/public/tables/t" && brokenTable:
			w.WriteHeader(http.StatusForbidden)
			write(map[string]any{"error": map[string]any{"message": "no", "type": "ForbiddenException", "code": 403}})
		case p == "wh/namespaces/public/tables/t":
			write(map[string]any{"metadata-location": "s3://bucket/warehouse/t/metadata/1.metadata.json", "metadata": json.RawMessage(tableMeta)})
		case p == "wh/namespaces/public/views/v":
			write(map[string]any{"metadata-location": "s3://bucket/warehouse/v/metadata/1.metadata.json", "metadata": json.RawMessage(viewMetadata)})
		default:
			w.WriteHeader(http.StatusNotFound)
			write(map[string]any{"error": map[string]any{"message": "no such table", "type": "NoSuchTableException", "code": 404}})
		}
	}))
	t.Cleanup(srv.Close)
	cat, err := Open(context.Background(), config.Config{Catalog: config.Catalog{URI: srv.URL, Warehouse: "wh"},
		Storage: config.Storage{Location: "s3://bucket/warehouse"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := cat.Locations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	if want := []string{"s3://bucket/warehouse/t", "s3://bucket/warehouse/v"}; !slices.Equal(got, want) {
		t.Errorf("locations %v, want %v", got, want)
	}
	brokenTable = true
	if got, err := cat.Locations(context.Background()); err == nil || !strings.Contains(err.Error(), "public.t") {
		t.Errorf("with a table that cannot be loaded: %v, %v", got, err)
	}
}
