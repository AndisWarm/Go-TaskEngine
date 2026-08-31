package redisstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-taskengine/internal/model"
)

func archiveTaskForTest(t *testing.T, store *Store, id string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	msg := message(id, 1, now)
	if err := store.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	active, err := store.Claim(ctx, msg.Queue, now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Archive(ctx, active, "invalid input"); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLetterListSupportsPaginationAndFields(t *testing.T) {
	store, _ := newTestStore(t)
	for _, id := range []string{"dead-a", "dead-b", "dead-c"} {
		archiveTaskForTest(t, store, id)
	}
	ctx := context.Background()
	page, err := store.ListDeadLetters(ctx, "default", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].ID != "dead-b" {
		t.Fatalf("page = %+v, want dead-b", page)
	}
	if page[0].State != model.StateArchived || page[0].LastError != "invalid input" {
		t.Fatalf("dead-letter fields = %+v", page[0])
	}
	if _, err := store.GetDeadLetter(ctx, "default", "dead-b"); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLetterReplayResetsRetryAndRequeuesAtomically(t *testing.T) {
	store, _ := newTestStore(t)
	archiveTaskForTest(t, store, "replay-1")
	ctx := context.Background()
	archived, err := store.GetDeadLetter(ctx, "default", "replay-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReplayDeadLetter(ctx, "default", archived.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := store.PendingCount(ctx, "default"); err != nil || got != 1 {
		t.Fatalf("pending after replay = %d, err=%v", got, err)
	}
	if _, err := store.GetDeadLetter(ctx, "default", archived.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("replayed dead-letter lookup error = %v, want ErrInvalidTransition", err)
	}
	pending, err := store.Claim(ctx, "default", time.Now(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if pending.RetryCount != 0 || pending.State != model.StateActive {
		t.Fatalf("replayed task = %+v", pending)
	}
}

func TestDeadLetterDeleteAndCleanup(t *testing.T) {
	store, _ := newTestStore(t)
	archiveTaskForTest(t, store, "delete-1")
	archiveTaskForTest(t, store, "cleanup-1")
	ctx := context.Background()
	if err := store.DeleteDeadLetter(ctx, "default", "delete-1"); err != nil {
		t.Fatal(err)
	}
	_, err := store.GetDeadLetter(ctx, "default", "delete-1")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("deleted lookup error = %v, want ErrTaskNotFound", err)
	}
	if !errors.Is(err, ErrNoTask) {
		t.Fatalf("deleted lookup error = %v, want ErrNoTask compatibility", err)
	}
	removed, err := store.CleanupDeadLetters(ctx, "default", time.Now().Add(time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("cleaned = %d, want 1", removed)
	}
	if got, err := store.ArchivedCount(ctx, "default"); err != nil || got != 0 {
		t.Fatalf("archived count after cleanup = %d, err=%v", got, err)
	}
}
