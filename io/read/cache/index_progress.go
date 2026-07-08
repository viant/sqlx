package cache

import "context"

type indexProgressKey struct{}

type IndexProgress struct {
	View    string
	Dataset string
	Case    string
}

func WithIndexProgress(ctx context.Context, progress *IndexProgress) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if progress == nil {
		return ctx
	}
	return context.WithValue(ctx, indexProgressKey{}, progress)
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
