package cache

import (
	"context"
	"time"
)

type indexProgressKey struct{}
type indexProgressCallbackKey struct{}

type IndexProgress struct {
	View    string
	Dataset string
	Case    string
}

type IndexProgressEvent struct {
	View    string
	Dataset string
	Case    string
	Column  string
	Rows    int
	Elapsed time.Duration
	Done    bool
}

type IndexProgressFn func(event *IndexProgressEvent)

func WithIndexProgress(ctx context.Context, progress *IndexProgress) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if progress == nil {
		return ctx
	}
	return context.WithValue(ctx, indexProgressKey{}, progress)
}

func WithIndexProgressCallback(ctx context.Context, callback IndexProgressFn) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if callback == nil {
		return ctx
	}
	return context.WithValue(ctx, indexProgressCallbackKey{}, callback)
}

func IndexProgressFromContext(ctx context.Context) (*IndexProgress, bool) {
	if ctx == nil {
		return nil, false
	}
	progress, ok := ctx.Value(indexProgressKey{}).(*IndexProgress)
	if !ok || progress == nil {
		return nil, false
	}
	return progress, true
}

func IndexProgressCallbackFromContext(ctx context.Context) (IndexProgressFn, bool) {
	if ctx == nil {
		return nil, false
	}
	callback, ok := ctx.Value(indexProgressCallbackKey{}).(IndexProgressFn)
	if !ok || callback == nil {
		return nil, false
	}
	return callback, true
}

func EmitIndexProgress(ctx context.Context, event *IndexProgressEvent) {
	if event == nil {
		return
	}
	progress, _ := IndexProgressFromContext(ctx)
	if progress != nil {
		if event.View == "" {
			event.View = progress.View
		}
		if event.Dataset == "" {
			event.Dataset = progress.Dataset
		}
		if event.Case == "" {
			event.Case = progress.Case
		}
	}
	callback, ok := IndexProgressCallbackFromContext(ctx)
	if !ok {
		return
	}
	callback(event)
}
