package main

import (
	"encoding/binary"
	"io"
)

func readLengthAndBytes(r io.Reader) ([]byte, error) {
	var l uint32
	err := binary.Read(r, binary.BigEndian, &l)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, l)
	for i := 0; i < len(buf); {
		n, err := r.Read(buf[i:])
		if err != nil {
			return nil, err
		}
		i += n
	}
	return buf, nil
}

func writeLengthAndBytes(w io.Writer, buf []byte) error {
	l := int32(len(buf))
	err := binary.Write(w, binary.BigEndian, l)
	if err != nil {
		return err
	}
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}
