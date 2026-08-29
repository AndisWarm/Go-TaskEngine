package model

import "testing"

func TestTaskStatesHaveStableNames(t *testing.T) {
	cases := map[TaskState]string{
		StatePending:   "pending",
		StateScheduled: "scheduled",
		StateActive:    "active",
		StateRetry:     "retry",
		StateArchived:  "archived",
		StateCompleted: "completed",
	}
	for state, want := range cases {
		if got := state.String(); got != want {
			t.Fatalf("state %q: got %q, want %q", state, got, want)
		}
	}
}
