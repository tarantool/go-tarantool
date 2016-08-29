package tarantool

import (
	"io"
	"errors"
)

type smallBuf struct {
	b []byte
	p int
}

func (s *smallBuf) Read(d []byte) (l int, err error) {
	l = len(s.b) - s.p
	if l == 0 && len(d) > 0 {
		return 0, io.EOF
	}
	if l > len(d) {
		l = len(d)
	}
	copy(d, s.b[s.p:])
	s.p += l
	return l, nil
}

func (s *smallBuf) ReadByte() (b byte, err error) {
	if s.p == len(s.b) {
		return 0, io.EOF
	}
	b = s.b[s.p]
	s.p++
	return b, nil
}

func (s *smallBuf) UnreadByte() error {
	if s.p == 0 {
		return errors.New("Could not unread")
	}
	s.p--
	return nil
}

func (s *smallBuf) Len() int {
	return len(s.b) - s.p
}

func (s *smallBuf) Bytes() []byte {
	if len(s.b) > s.p {
		return s.b[s.p:]
	}
	return nil
}

type smallWBuf []byte

func (s *smallWBuf) Write(b []byte) (int, error) {
	*s = append(*s, b...)
	return len(b), nil
}

func (s *smallWBuf) WriteByte(b byte) error {
	*s = append(*s, b)
	return nil
}

func (s *smallWBuf) WriteString(ss string) (int, error) {
	*s = append(*s, ss...)
	return len(ss), nil
}
