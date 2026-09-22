package cache

import "sync"

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
