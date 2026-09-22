package aerospike

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go"
)

func TestConfiguredProviderValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"missing_host", func(c *Config) { c.Provider = "aerospike://:3000/test" }},
		{"missing_port", func(c *Config) { c.Provider = "aerospike://localhost/test" }},
		{"port_range", func(c *Config) { c.Provider = "aerospike://localhost:65536/test" }},
		{"missing_namespace", func(c *Config) { c.Provider = "aerospike://localhost:3000" }},
		{"extra_path", func(c *Config) { c.Provider += "/extra" }},
		{"userinfo", func(c *Config) { c.Provider = "aerospike://user:password@localhost:3000/test" }},
		{"query", func(c *Config) { c.Provider += "?x=1" }},
		{"fragment", func(c *Config) { c.Provider += "#x" }},
		{"bad_escape", func(c *Config) { c.Provider = "aerospike://localhost:3000/%zz" }},
		{"wrong_scheme", func(c *Config) { c.Provider = "http://localhost:3000/test" }},
		{"set_empty", func(c *Config) { c.Location = "" }},
		{"set_long", func(c *Config) { c.Location = strings.Repeat("a", 64) }},
		{"ttl_fraction", func(c *Config) { c.TTL = 1500 * time.Millisecond }},
		{"ttl_zero", func(c *Config) { c.TTL = 0 }},
		{"ttl_preserve", func(c *Config) { c.TTL = time.Duration(^uint32(0)-1) * time.Second }},
		{"ttl_special", func(c *Config) { c.TTL = time.Duration(^uint32(0)) * time.Second }},
		{"timeout_negative", func(c *Config) { c.Timeout.SocketTimeoutMs = -1 }},
		{"timeout_overflow", func(c *Config) { c.Timeout.TotalTimeoutMs = int(^uint(0) >> 1) }},
		{"reset_missing", func(c *Config) { c.FailedRequestLimit = 1 }},
		{"limit_missing", func(c *Config) { c.ResetFailuresInMs = 10 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "timeout_overflow" && strconv.IntSize < 64 {
				t.Skip("duration overflow requires 64-bit int")
			}
			c := Config{Provider: "aerospike://localhost:3000/test", Location: "cache", TTL: time.Minute}
			tc.change(&c)
			if _, err := c.validate(); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
	for _, uri := range []string{"aerospike://localhost:3000/test", "aerospike://[::1]:3000/test"} {
		if _, err := (Config{Provider: uri, Location: "cache", TTL: time.Second}).validate(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConfiguredIdentityAndTimeout(t *testing.T) {
	var keys []string
	for _, identity := range []string{"component/root/left", "component/root/right", ""} {
		c := Config{Identity: identity}
		service := &Cache{identityPrefix: c.keyPrefix()}
		key, err := service.identityURL("SELECT id FROM records", []any{1}, nil)
		if err != nil {
			t.Fatal(err)
		}
		warm, err := service.identityURL("SELECT id FROM records", nil, []byte("[1]"))
		if err != nil || key != warm {
			t.Fatalf("lazy/warmup identity %s/%s %v", key, warm, err)
		}
		keys = append(keys, key)
	}
	if keys[0] == keys[1] || keys[0] == keys[2] {
		t.Fatal("identity collision")
	}
	c := &Cache{timeoutConfig: &TimeoutConfig{MaxRetries: 4, TotalTimeoutMs: 500, SocketTimeoutMs: 100, SleepBetweenRetriesMs: 20}}
	p := c.newBasePolicy(true)
	if p.MaxRetries != 4 || p.TotalTimeout != 500*time.Millisecond || p.SocketTimeout != 100*time.Millisecond || p.SleepBetweenRetries != 20*time.Millisecond {
		t.Fatalf("policy=%+v", p)
	}
	if c.newBasePolicy(false).MaxRetries != as.NewPolicy().MaxRetries {
		t.Fatal("write retry behavior changed")
	}
}

func TestConfiguredFailureLifetime(t *testing.T) {
	reset := 20 * time.Millisecond
	f := NewFailureHandler(1, &reset)
	f.HandleFailure()
	if f.IsProbing() {
		t.Fatal("threshold changed")
	}
	f.HandleFailure()
	if !f.IsProbing() {
		t.Fatal("missing probe")
	}
	deadline := time.Now().Add(time.Second)
	for f.IsProbing() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if f.IsProbing() {
		t.Fatal("reset did not run")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				f.HandleFailure()
				f.HandleSuccess()
				_ = f.IsProbing()
			}
		}()
	}
	_ = f.Close()
	wg.Wait()
	_ = f.Close()
	f.mux.Lock()
	defer f.mux.Unlock()
	if f.timer != nil || !f.closed {
		t.Fatal("failure timer retained after close")
	}
}

func TestConfiguredPoolClosed(t *testing.T) {
	var p Pool
	_ = p.Close()
	_ = p.Close()
	if _, err := p.NewCache(Config{Provider: "aerospike://localhost:3000/test", Location: "cache", TTL: time.Second}); err == nil {
		t.Fatal("closed pool reopened")
	}
}

// Opt-in only: point at the dedicated validation container, never production.
func TestConfiguredPoolAerospike(t *testing.T) {
	uri := os.Getenv("DATLY_TEST_AEROSPIKE")
	if uri == "" {
		t.Skip("DATLY_TEST_AEROSPIKE is unset")
	}
	var p Pool
	t.Cleanup(func() { _ = p.Close() })
	c := Config{Provider: uri, Location: "datly_validation", Identity: t.Name(), TTL: 30 * time.Second, FailedRequestLimit: 1, ResetFailuresInMs: 1000}
	first, err := p.NewCache(c)
	if err != nil {
		t.Fatal(err)
	}
	same, err := p.NewCache(c)
	if err != nil || same != first {
		t.Fatalf("pool reuse=%p,%v", same, err)
	}
	c.Identity += "/other"
	other, err := p.NewCache(c)
	if err != nil {
		t.Fatal(err)
	}
	if first == other || first.client != other.client || len(p.clients) != 1 {
		t.Fatal("expected scoped caches with shared client")
	}
	first.failureHandler.HandleFailure()
	first.failureHandler.HandleFailure()
	client := first.client
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if client.IsConnected() {
		t.Fatal("client remains connected")
	}
	if first.failureHandler.timer != nil {
		t.Fatal("timer not closed")
	}
	if _, err := p.NewCache(c); err == nil {
		t.Fatal("closed pool reopened")
	}
}
