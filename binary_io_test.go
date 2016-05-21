package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestReadLengthAndBytes(t *testing.T) {
	rbuf := new(bytes.Buffer)
	l := int64(len("hello, world"))
	_, err := WriteVariant(rbuf, l)
	if err != nil {
		t.Error(err)
	}
	_, err = rbuf.WriteString("hello, world")
	if err != nil {
		t.Error(err)
	}

	l = int64(len("goodbye, world"))
	_, err = WriteVariant(rbuf, l)
	if err != nil {
		t.Error(err)
	}
	_, err = rbuf.WriteString("goodbye, world")
	if err != nil {
		t.Error(err)
	}

	r := bytes.NewBuffer(rbuf.Bytes())
	buf, n, err := ReadLengthAndBytes(r)
	if err != nil {
		t.Errorf("readLengthAndBytes failed. err=%s", err)
	}
	if n != 1+len(buf) {
		t.Errorf("unexpected read bytes. got=%v; want=%v", n, 1+len(buf))
	}
	got := string(buf)
	want := "hello, world"
	if got != want {
		t.Errorf("unexpected result. got=%s; want=%s", got, want)
	}

	buf, n, err = ReadLengthAndBytes(r)
	if err != nil {
		t.Errorf("readLengthAndBytes failed. err=%s", err)
	}
	if n != 1+len(buf) {
		t.Errorf("unexpected read bytes. got=%v; want=%v", n, 1+len(buf))
	}
	got = string(buf)
	want = "goodbye, world"
	if got != want {
		t.Errorf("unexpected result. got=%s; want=%s", got, want)
	}
}

func TestWriteLengthAndBytes(t *testing.T) {
	w := new(bytes.Buffer)
	buf := []byte("hello, world")
	n, err := WriteLengthAndBytes(w, buf)
	if err != nil {
		t.Errorf("writeLengthAndBytes failed. err=%s", err)
	}
	if n != 1+len(buf) {
		t.Errorf("unexpected written bytes. got=%v; want=%v", n, 1+len(buf))
	}
	buf = []byte("goodbye, world")
	n, err = WriteLengthAndBytes(w, buf)
	if err != nil {
		t.Errorf("writeLengthAndBytes failed. err=%s", err)
	}
	if n != 1+len(buf) {
		t.Errorf("unexpected written bytes. got=%v; want=%v", n, 1+len(buf))
	}

	r := bytes.NewBuffer(w.Bytes())
	l, err := binary.ReadVarint(r)
	if err != nil {
		t.Error(err)
	}
	if l != int64(len("hello, world")) {
		t.Errorf("unexpected result. got=%s; want=%s", l, int64(len("hello, world")))
	}
	buf = make([]byte, l)
	_, err = io.ReadFull(r, buf)
	got := string(buf)
	want := "hello, world"
	if got != want {
		t.Errorf("unexpected result. got=%s; want=%s", got, want)
	}
	l, err = binary.ReadVarint(r)
	if err != nil {
		t.Error(err)
	}
	if err != nil {
		t.Error(err)
	}
	if l != int64(len("goodbye, world")) {
		t.Errorf("unexpected result. got=%s; want=%s", l, int64(len("hello, world")))
	}
	buf = make([]byte, l)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		t.Error(err)
	}
	got = string(buf)
	want = "goodbye, world"
	if got != want {
		t.Errorf("unexpected result. got=%s; want=%s", got, want)
	}
}
