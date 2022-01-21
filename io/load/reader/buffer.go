package reader

//Buffer represents buffer
type Buffer struct {
	buffer []byte
	offset int
}

//NewBuffer creates a buffer instance with given initial size
func NewBuffer(size int) *Buffer {
	return &Buffer{
		buffer: make([]byte, size),
	}
}

//WriteString add string to the buffer
func (b *Buffer) WriteString(value string) {
	if len(value)+b.offset > len(b.buffer) {
		b.buffer = append(b.buffer[:b.offset], []byte(value)...)
		b.offset = len(b.buffer)
		return
	}

	b.offset += copy(b.buffer[b.offset:], value)
}

//Len returns actual buffer len
func (b *Buffer) Len() int {
	return b.offset
}

//Reset sets actual buffer len to 0
func (b *Buffer) Reset() {
	b.offset = 0
}

//Read tries to read actual buffer to dest, resets actual buffer size if succeed
func (b *Buffer) Read(dest []byte) (int, bool) {
	if b.offset == 0 {
		return 0, true
	}

	if len(dest) < b.offset {
		return 0, false
	}

	offset := copy(dest, b.buffer[:b.offset])
	b.Reset()
	return offset, true
}
