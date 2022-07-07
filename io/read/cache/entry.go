package cache

import (
	"bufio"
	"context"
	"encoding/json"
	goIo "io"
)

type Entry struct {
	Meta Meta
	Data []byte // Entry is used as Iterator, Data is last streamed line.

	index    int
	rowAdded bool

	reader      *bufio.Reader
	writer      *bufio.Writer
	writeCloser goIo.WriteCloser
	readCloser  goIo.ReadCloser
}

func (c *Service) addRow(ctx context.Context, e *Entry, values []interface{}) error {
	if len(values) == 0 {
		return nil
	}

	if err := c.writeMetaIfNeeded(ctx, e); err != nil {
		return err
	}

	marshal, err := json.Marshal(values)
	if err != nil {
		return err
	}

	err = c.write(e.writer, marshal, true)
	if err != nil {
		return err
	}

	return nil
}

func (c *Service) writeMetaIfNeeded(ctx context.Context, e *Entry) error {
	if e.rowAdded {
		return nil
	}

	var err error
	e.writer, err = c.writeMeta(ctx, e)
	if err != nil {
		return e.writeCloser.Close()
	}

	e.rowAdded = true
	return nil
}

func (e *Entry) Next() bool {
	line, err := readLine(e.reader)
	e.Data = line

	return err == nil
}

func (e *Entry) Has() bool {
	return e.reader != nil
}
