package cache

import (
	"io"
)

type Writer interface {
	io.Writer
	Flush() error
}

type (
	WriteCloser struct {
		writer Writer
		closer io.Closer
	}

	LineWriter struct {
		addNewLine bool
		writer     Writer
	}
)

func (l *LineWriter) Flush() error {
	return l.writer.Flush()
}

func NewLineWriter(writer Writer) Writer {
	return &LineWriter{writer: writer}
}

func (l *LineWriter) Write(p []byte) (n int, err error) {
	if l.addNewLine {
		if n, err = l.writer.Write([]byte("\n")); err != nil {
			return n, err
		}
	}

	l.addNewLine = true
	return l.writer.Write(p)
}

func NewWriteCloser(writer Writer, closer io.Closer) *WriteCloser {
	return &WriteCloser{
		writer: writer,
		closer: closer,
	}
}

func (w *WriteCloser) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

func (w *WriteCloser) Close() error {
	return w.closer.Close()
}

func (w *WriteCloser) Flush() error {
	return w.writer.Flush()
}
