package cache

import "io"

type Reader interface {
	io.Reader
	LineReader
}

type (
	ReadCloser struct {
		reader Reader
		closer io.Closer
	}
)

func (w *ReadCloser) ReadLine() (line []byte, prefix bool, err error) {
	return w.reader.ReadLine()
}

func NewReadCloser(reader Reader, closer io.Closer) *ReadCloser {
	return &ReadCloser{
		reader: reader,
		closer: closer,
	}
}

func (w *ReadCloser) Read(p []byte) (n int, err error) {
	return w.reader.Read(p)
}

func (w *ReadCloser) Close() error {
	return w.closer.Close()
}
