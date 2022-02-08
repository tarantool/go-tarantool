package tarantool

import (
	"errors"
	"io"
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

type smallWBuf struct {
	b   []byte
	sum uint
	n   uint
}

func (s *smallWBuf) Write(b []byte) (int, error) {
	s.b = append(s.b, b...)
	return len(s.b), nil
}

func (s *smallWBuf) WriteByte(b byte) error {
	s.b = append(s.b, b)
	return nil
}

func (s *smallWBuf) WriteString(ss string) (int, error) {
	s.b = append(s.b, ss...)
	return len(ss), nil
}

func (s smallWBuf) Len() int {
	return len(s.b)
}

func (s smallWBuf) Cap() int {
	return cap(s.b)
}

func (s *smallWBuf) Trunc(n int) {
	s.b = s.b[:n]
}

func (s *smallWBuf) Reset() {
	s.sum = uint(uint64(s.sum)*15/16) + uint(len(s.b))
	if s.n < 16 {
		s.n++
	}
	if cap(s.b) > 1024 && s.sum/s.n < uint(cap(s.b))/4 {
		s.b = make([]byte, 0, s.sum/s.n)
	} else {
		s.b = s.b[:0]
	}
}
