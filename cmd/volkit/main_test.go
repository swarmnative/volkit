package main

import (
	"os"
	"testing"
)

func TestCheckStartupEnv_AgentMissingCoordinatorURL(t *testing.T) {
	// backup and restore env
	oldMode := os.Getenv("VOLKIT_MODE")
	oldURL := os.Getenv("VOLKIT_COORDINATOR_URL")
	t.Cleanup(func() {
		_ = os.Setenv("VOLKIT_MODE", oldMode)
		if oldURL == "" { _ = os.Unsetenv("VOLKIT_COORDINATOR_URL") } else { _ = os.Setenv("VOLKIT_COORDINATOR_URL", oldURL) }
	})

	// agent mode without coordinator URL should error
	_ = os.Setenv("VOLKIT_MODE", "agent")
	_ = os.Unsetenv("VOLKIT_COORDINATOR_URL")
	if err := checkStartupEnv(); err == nil {
		t.Fatal("expected error when VOLKIT_MODE=agent and VOLKIT_COORDINATOR_URL is missing")
	}

	// when URL set, should pass
	_ = os.Setenv("VOLKIT_COORDINATOR_URL", "http://coordinator:8080")
	if err := checkStartupEnv(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
