package cache

import (
	"context"
	"testing"
	"time"
)

func TestEmitIndexProgress(t *testing.T) {
	ctx := WithIndexProgress(context.Background(), &IndexProgress{
		View:    "performanceTimeline",
		Dataset: "ns_memory/performanceTimeline_dataset",
		Case:    "Period=today,Granularity=hour",
	})

	var actual *IndexProgressEvent
	ctx = WithIndexProgressCallback(ctx, func(event *IndexProgressEvent) {
		if event == nil {
			return
		}
		cloned := *event
		actual = &cloned
	})

	EmitIndexProgress(ctx, &IndexProgressEvent{
		Column:  "order_id",
		Rows:    42,
		Elapsed: 2 * time.Second,
		Done:    true,
	})

	if actual == nil {
		t.Fatalf("expected callback event")
	}
	if actual.View != "performanceTimeline" {
		t.Fatalf("unexpected view: %v", actual.View)
	}
	if actual.Dataset != "ns_memory/performanceTimeline_dataset" {
		t.Fatalf("unexpected dataset: %v", actual.Dataset)
	}
	if actual.Case != "Period=today,Granularity=hour" {
		t.Fatalf("unexpected case: %v", actual.Case)
	}
	if actual.Column != "order_id" {
		t.Fatalf("unexpected column: %v", actual.Column)
	}
	if actual.Rows != 42 {
		t.Fatalf("unexpected rows: %v", actual.Rows)
	}
	if actual.Elapsed != 2*time.Second {
		t.Fatalf("unexpected elapsed: %v", actual.Elapsed)
	}
	if !actual.Done {
		t.Fatalf("expected done event")
	}
}
