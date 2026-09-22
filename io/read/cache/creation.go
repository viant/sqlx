package cache

import (
	"context"
	"sync"
)

const (
	CreationLazy   = "lazy"
	CreationWarmup = "warmup"
)

// CreationMetrics observes successful native cache publications. Entries counts
// logical query/group entries and index markers, excluding overflow chunks.
// Reused entries and rolled-back publications do not increment this metric.
type CreationMetrics struct {
	mu       sync.RWMutex
	observer func(kind string, entries int)
}

// SetCreationObserver installs a thread-safe publication observer. The callback
// runs synchronously after publication and must tolerate concurrent calls.
func (m *CreationMetrics) SetCreationObserver(observer func(kind string, entries int)) {
	m.mu.Lock()
	m.observer = observer
	m.mu.Unlock()
}
func (m *CreationMetrics) RecordCreation(kind string, entries int) {
	if entries <= 0 {
		return
	}
	m.mu.RLock()
	observer := m.observer
	m.mu.RUnlock()
	if observer != nil {
		observer(kind, entries)
	}
}

type creationObserverKey struct{}

// WithCreationObserver adds an invocation-local observer without changing the
// cache's configured observer or ownership. Both observers receive publications.
func WithCreationObserver(ctx context.Context, observer func(string, int)) context.Context {
	return context.WithValue(ctx, creationObserverKey{}, observer)
}
func (m *CreationMetrics) RecordCreationContext(ctx context.Context, kind string, entries int) {
	m.RecordCreation(kind, entries)
	if entries <= 0 || ctx == nil {
		return
	}
	if observer, ok := ctx.Value(creationObserverKey{}).(func(string, int)); ok && observer != nil {
		observer(kind, entries)
	}
}
