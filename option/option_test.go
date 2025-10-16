package option

import (
    "sync"
    "testing"
)

// Test that Options.MetaSessionCache returns nil when no cache option is provided.
func TestOptions_MetaSessionCache_Empty(t *testing.T) {
    var opts Options
    if got := opts.MetaSessionCache(); got != nil {
        t.Fatalf("expected nil cache, got: %#v", got)
    }

    opts = Options{Identity("id")}
    if got := opts.MetaSessionCache(); got != nil {
        t.Fatalf("expected nil cache with unrelated options, got: %#v", got)
    }
}

// Test that WithMetaSessionCache stores and Options.MetaSessionCache retrieves the same map instance.
func TestOptions_WithMetaSessionCache(t *testing.T) {
    cache := &sync.Map{}
    opts := Options{WithMetaSessionCache(cache)}

    got := opts.MetaSessionCache()
    if got == nil {
        t.Fatalf("expected non-nil cache, got nil")
    }
    if got != cache {
        t.Fatalf("expected same cache pointer, got %p want %p", got, cache)
    }

    // Verify the returned map is functional and is the same underlying map
    key, val := "k", "v"
    got.Store(key, val)
    if v, ok := cache.Load(key); !ok || v != val {
        t.Fatalf("expected to load value %q from original cache, got (%v, %v)", val, v, ok)
    }
}

// Test WithMetaSessionCache with a nil map yields nil from Options.MetaSessionCache.
func TestOptions_WithMetaSessionCache_Nil(t *testing.T) {
    opts := Options{WithMetaSessionCache(nil)}
    if got := opts.MetaSessionCache(); got != nil {
        t.Fatalf("expected nil cache when constructed with nil, got: %#v", got)
    }
}

// Test Assign can extract MetaSessionCacheKey from options.
func TestAssign_MetaSessionCacheKey_Assigns(t *testing.T) {
    var key MetaSessionCacheKey
    opts := Options{MetaSessionCacheKey("meta-key")}
    assigned := Assign(opts, &key)
    if !assigned {
        t.Fatalf("expected assigned=true, got false")
    }
    if got, want := string(key), "meta-key"; got != want {
        t.Fatalf("unexpected key value, got %q want %q", got, want)
    }
}

// Test Assign leaves key as zero value when not present.
func TestAssign_MetaSessionCacheKey_NotPresent(t *testing.T) {
    var key MetaSessionCacheKey
    opts := Options{}
    assigned := Assign(opts, &key)
    if assigned {
        t.Fatalf("expected assigned=false, got true")
    }
    if key != "" {
        t.Fatalf("expected zero value key, got %q", string(key))
    }
}

