package main

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestReadLengthAndBytes(t *testing.T) {
	l := int32(len("hello, world"))
	rbuf := new(bytes.Buffer)
	err := binary.Write(rbuf, binary.BigEndian, l)
	if err != nil {
		t.Error(err)
	}
	_, err = rbuf.WriteString("hello, world")
	if err != nil {
		t.Error(err)
	}

	r := bytes.NewBuffer(rbuf.Bytes())
	buf, err := readLengthAndBytes(r)
	if err != nil {
		t.Errorf("readLengthAndBytes failed. err=%s", err)
	}
	got := string(buf)
	want := "hello, world"
	if got != want {
		t.Errorf("unexpected result. got=%s; want=%s", got, want)
	}
}

func TestWriteLengthAndBytes(t *testing.T) {
	buf := []byte("hello, world")
	w := new(bytes.Buffer)
	err := writeLengthAndBytes(w, buf)
	if err != nil {
		t.Errorf("writeLengthAndBytes failed. err=%s", err)
	}

	r := bytes.NewBuffer(w.Bytes())
	var l int32
	err = binary.Read(r, binary.BigEndian, &l)
	if err != nil {
		t.Error(err)
	}
	if l != int32(len("hello, world")) {
		t.Errorf("unexpected result. got=%s; want=%s", l, int32(len("hello, world")))
	}
	got := r.String()
	want := "hello, world"
	if got != want {
		t.Errorf("unexpected result. got=%s; want=%s", got, want)
	}
}
