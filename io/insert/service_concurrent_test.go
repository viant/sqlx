package insert

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewSession_FreshUpdatersPerCall verifies that reusing a cached
// session does not share numericSequencer instances across sessions.
// Sharing led to a nil-pointer panic in updateRecord when one goroutine
// reset n.sequenceValue while another was mid-flight.
func TestNewSession_FreshUpdatersPerCall(t *testing.T) {
	type entity struct {
		ID   int `sqlx:"name=foo_id,primaryKey=true,generator=autoincrement"`
		Name string
	}

	svc := &Service{tableName: "t"}

	cached := &session{rType: reflect.TypeOf(&entity{}), batchSize: 1}
	cached.recordUpdaters = []recordUpdater{
		newNumericSequencer(cached, nil, 0),
	}
	svc.cachedSession = cached

	s1, err := svc.NewSession(context.Background(), &entity{}, nil, 1)
	require.NoError(t, err)
	s2, err := svc.NewSession(context.Background(), &entity{}, nil, 1)
	require.NoError(t, err)

	require.Len(t, s1.recordUpdaters, 1)
	require.Len(t, s2.recordUpdaters, 1)
	require.NotSame(t, s1.recordUpdaters[0], s2.recordUpdaters[0],
		"each session must own its numericSequencer")
	require.NotSame(t, s1.recordUpdaters[0], cached.recordUpdaters[0],
		"cached sequencer must not be reused by a returned session")

	n1 := s1.recordUpdaters[0].(*numericSequencer)
	n2 := s2.recordUpdaters[0].(*numericSequencer)
	require.Same(t, s1, n1.session)
	require.Same(t, s2, n2.session)
	require.False(t, n1.detectedPreset)
	require.False(t, n2.detectedPreset)
	require.Nil(t, n1.sequenceValue)
	require.Nil(t, n2.sequenceValue)
}

// TestNewSession_ConcurrentNoSharedState runs NewSession from many
// goroutines and asserts every returned session has its own sequencer
// pointer, which is what eliminates the upstream race that caused the
// production nil-pointer panic.
func TestNewSession_ConcurrentNoSharedState(t *testing.T) {
	type entity struct {
		ID   int `sqlx:"name=foo_id,primaryKey=true,generator=autoincrement"`
		Name string
	}

	svc := &Service{tableName: "t"}
	cached := &session{rType: reflect.TypeOf(&entity{}), batchSize: 1}
	cached.recordUpdaters = []recordUpdater{
		newNumericSequencer(cached, nil, 0),
	}
	svc.cachedSession = cached

	const goroutines = 64
	seen := make([]*numericSequencer, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			s, err := svc.NewSession(context.Background(), &entity{}, nil, 1)
			if err != nil {
				t.Errorf("NewSession: %v", err)
				return
			}
			seen[idx] = s.recordUpdaters[0].(*numericSequencer)
		}(i)
	}
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		require.NotNil(t, seen[i])
		for j := i + 1; j < goroutines; j++ {
			require.NotSame(t, seen[i], seen[j],
				"goroutines %d and %d share a sequencer", i, j)
		}
	}
}
