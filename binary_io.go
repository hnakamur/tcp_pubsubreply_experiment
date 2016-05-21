package main

import (
	"encoding/binary"
	"io"
)

type nonBufferedByteReader struct {
	reader    io.Reader
	buf       []byte
	bytesRead int
}

func NewNonBufferedByteReader(r io.Reader) *nonBufferedByteReader {
	return &nonBufferedByteReader{
		reader: r,
		buf:    make([]byte, 1),
	}
}

func (r *nonBufferedByteReader) ReadByte() (c byte, err error) {
	n, err := r.reader.Read(r.buf)
	r.bytesRead += n
	if n > 0 {
		c = r.buf[0]
	}
	return
}

func (r *nonBufferedByteReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.bytesRead += n
	return
}

func (r *nonBufferedByteReader) BytesRead() int {
	return r.bytesRead
}

func ReadLengthAndBytes(r io.Reader) (buf []byte, n int, err error) {
	br := NewNonBufferedByteReader(r)
	l, err := binary.ReadVarint(br)
	if err != nil {
		return nil, br.BytesRead(), err
	}
	buf = make([]byte, l)
	_, err = io.ReadFull(br, buf)
	if err != nil {
		return nil, br.BytesRead(), err
	}
	return buf, br.BytesRead(), nil
}

func WriteLengthAndBytes(w io.Writer, buf []byte) (n int, err error) {
	n, err = WriteVariant(w, int64(len(buf)))
	if err != nil {
		return
	}
	n2, err := WriteFull(w, buf)
	return n + n2, err
}

func WriteVariant(w io.Writer, v int64) (int, error) {
	buf := make([]byte, binary.Size(v))
	n := binary.PutVarint(buf, v)
	return w.Write(buf[:n])
}

func WriteFull(w io.Writer, buf []byte) (n int, err error) {
	for len(buf) > 0 {
		var n2 int
		n2, err = w.Write(buf)
		n += n2
		if err != nil {
			return
		}
		buf = buf[n2:]
	}
	return
}
