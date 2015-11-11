package resv

import (
	"errors"
	"io"
)

const (
	opInvalid int = iota
	opRead
	opUnread
)

type scanner struct {
	last    byte
	lastErr error
	op      int

	r io.Reader
}

func (s *scanner) Read(o []byte) (n int, err error) {
	n, err = s.r.Read(o)
	if n > 0 {
		s.last = o[n-1]
		s.lastErr = err
		s.op = opRead
	} else {
		s.last = 0
		s.lastErr = err
		s.op = opInvalid
	}
	return n, err
}

func (s *scanner) UnreadByte() error {
	if s.op == opRead {
		s.op = opUnread
		return nil
	}

	return errors.New("scanner: last op was not unread")
}

func (s *scanner) ReadByte() (byte, error) {
	if s.op == opUnread {
		s.op = opRead
		return s.last, nil
	}

	var b [1]byte
	n, err := s.Read(b[:])
	if n == 0 && err == nil {
		err = io.EOF
	}

	return b[0], err
}

func (s *scanner) ReadBytes(delim byte) (line []byte, err error) {
	for {
		b, err := s.ReadByte()
		if err != nil {
			return line, err
		}

		line = append(line, b)
		if b == delim {
			break
		}
	}
	return line, err
}
