package server_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestServerProductionCodeDoesNotImportRedisStore(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Clean(name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", name, err)
			}
			if path == "go-taskengine/redisstore" {
				t.Fatalf("production file %s imports Redis implementation", name)
			}
		}
	}
}

func TestServerStorageCallsDoNotUseBackgroundContext(t *testing.T) {
	source, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	methods := []string{
		"MoveReady", "PendingCount", "Claim", "Requeue", "AckSuccess",
		"ScheduleRetry", "Archive", "ExtendLease", "ExpiredIDs", "Get",
	}
	for _, method := range methods {
		forbidden := "s.store." + method + "(context.Background()"
		if strings.Contains(text, forbidden) {
			t.Errorf("server storage call %s uses context.Background", method)
		}
	}
}
