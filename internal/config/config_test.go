package config

import (
	"testing"
)

func TestConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test:test@localhost:5432/test")
	t.Setenv("API_KEYS", "alice:secret-a,bob:secret-b")
	c, e := Load()
	if e != nil || len(c.Keys) != 2 || c.Concurrency != 4 {
		t.Fatal(c, e)
	}

	t.Setenv("HEARTBEAT", "40s")
	if _, e = Load(); e == nil {
		t.Fatal("heartbeat greater than lease accepted")
	}
}

func TestKeyValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test:test@localhost:5432/test")

	for _, s := range []string{"", "alice:same,bob:same", "alice:a,alice:b"} {
		t.Setenv("API_KEYS", s)
		if _, e := Load(); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}
