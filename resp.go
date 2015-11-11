package fred

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
)

type BadTypeError byte

func (b BadTypeError) Error() string {
	return fmt.Sprintf("unrecognized type character %q", byte(b))
}

type Type int

const (
	SimpleStr Type = 1 << iota
	BulkStr
	Err
	Int
	Array
	Nil
)

const (
	Str     = SimpleStr | BulkStr
	Invalid = ^(Str | Err | Int | Array | Nil)
)

func (t Type) String() string {
	switch t {
	case SimpleStr:
		return "SimpleStr"
	case BulkStr:
		return "BulkStr"
	case Err:
		return "Err"
	case Int:
		return "Int"
	case Array:
		return "Array"
	case Nil:
		return "Nil"
	}
	return "Invalid"
}

func (t Type) GoString() string {
	return fmt.Sprintf("%T(%v)", t, t)
}

type Resp struct {
	typ   Type
	value interface{}

	Err error
}

func (r Resp) Bytes() ([]byte, error) {
	if r.IsType(Nil) {
		return nil, nil
	}

	if b, ok := r.value.([]byte); ok && r.IsType(Str) {
		bc := make([]byte, len(b))
		copy(bc, b)
		return bc, nil
	}
	return nil, ErrWrongType
}

func (r Resp) BytesList() ([][]byte, error) {
	ary, err := r.Array()
	if err != nil {
		return nil, err
	}

	result := make([][]byte, len(ary))
	for i, re := range ary {
		result[i], err = re.Bytes()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (r Resp) IsType(t Type) bool {
	if r.typ == 0 && t == Invalid {
		return true
	}
	return r.typ&t != 0
}

func (r Resp) Str() (string, error) {
	if r.Err != nil {
		return "", r.Err
	} else if r.IsType(Nil) {
		return "", nil
	}

	if bs, ok := r.value.([]byte); ok && r.IsType(Str) {
		return string(bs), nil
	}
	return "", ErrWrongType
}

func (r Resp) StrList() ([]string, error) {
	ary, err := r.Array()
	if err != nil {
		return nil, err
	}

	result := make([]string, len(ary))
	for i, re := range ary {
		result[i], err = re.Str()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (r Resp) Array() ([]Resp, error) {
	if r.Err != nil {
		return nil, r.Err
	}

	if ary, ok := r.value.([]Resp); ok && r.IsType(Array) {
		return ary, nil
	}
	// As a special case, if r is not an array, it will return itself in an array.
	return []Resp{r}, nil
}

func (r Resp) Int() (int64, error) {
	if r.Err != nil {
		return 0, r.Err
	}

	if i, ok := r.value.(int64); ok && r.IsType(Int) {
		return i, nil
	}
	return 0, ErrWrongType
}

var (
	ErrNoCRLF                = errors.New("no CRLF found")
	ErrMalformedInt          = errors.New("integer response is malformed")
	ErrMalformedBulkString   = errors.New("bulk string response is malformed: no trailing CRLF")
	ErrMalformedSimpleString = errors.New("simple string response is malformed: contained CR or LF")
	ErrBadSize               = errors.New("bulk string response is malformed: size is negative")
	ErrByteConversion        = errors.New("Resp.Bytes: cannot convert resp to []byte")
	ErrWrongType             = errors.New("resp is not of that type")
)

func toFloat(val interface{}, bits int) (float64, error) {
	switch val := val.(type) {
	case int64:
		return float64(val), nil
	case Error:
		return 0, val
	case []byte:
		return strconv.ParseFloat(string(val), bits)
	}
	return 0, errWrongType
}

func toInt(val interface{}, bits int) (int64, error) {
	switch val := val.(type) {
	case int64:
		return val, nil
	case Error:
		return 0, val
	case []byte:
		return strconv.ParseInt(string(val), 0, bits)
	}
	return 0, errWrongType
}

func toUint(val interface{}, bits int) (uint64, error) {
	switch val := val.(type) {
	case int64:
		return uint64(val), nil
	case Error:
		return 0, val
	case []byte:
		return strconv.ParseUint(string(val), 0, bits)
	}
	return 0, errWrongType
}

func toString(val interface{}) (string, error) {
	switch val := val.(type) {
	case int64:
		return strconv.FormatInt(val, 10), nil
	case Error:
		return "", val
	case []byte:
		return string(val), nil
	}
	return "", errWrongType
}

func toBytes(val interface{}) ([]byte, error) {
	switch val := val.(type) {
	case int64:
		return []byte(strconv.FormatInt(val, 10)), nil
	case Error:
		return nil, val
	case []byte:
		b := make([]byte, len(val))
		copy(b, val)
		return b, nil
	}
	return nil, errWrongType
}

type Error string

func (e Error) Error() string {
	return string(e)
}

type byteScanner interface {
	io.Reader
	io.ByteScanner
	ReadBytes(delim byte) ([]byte, error)
}

func scanner(r io.Reader) byteScanner {
	if r, ok := r.(byteScanner); ok {
		return r
	}

	return bufio.NewReader(r)
}

func readCRLF(r byteScanner, err error) error {
	for c := byte('\r'); ; c = '\n' {
		if b, err := r.ReadByte(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		} else if c != b {
			return ErrNoCRLF
		} else if c == '\n' {
			return nil
		}
	}
}

func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func readSimpleString(r byteScanner) ([]byte, error) {
	var line []byte
	for {
		next, err := r.ReadBytes('\r')
		if err != nil {
			return line, unexpectedEOF(err)
		} else if bytes.IndexByte(next, '\n') != -1 {
			return nil, ErrMalformedSimpleString
		} else if line == nil {
			line = next
		} else {
			line = append(line, next...)
		}

		// Short read?
		if n := len(next); n > 0 && next[n-1] != '\r' {
			continue
		}

		if b, err := r.ReadByte(); err != nil {
			return line, unexpectedEOF(err)
		} else if b == '\n' {
			line = line[:len(line)-1]
			return line, nil
		} else {
			return nil, ErrMalformedSimpleString
		}
	}
}

func readError(r byteScanner) (errmsg error, err error) {
	msg, err := readSimpleString(r)
	if err != nil {
		return nil, err
	}
	return Error(string(msg)), nil
}

func readInteger(r byteScanner) (i int64, err error) {
	b, err := readSimpleString(r)
	if err != nil {
		return 0, err
	}

	n := 0
	neg := b[0] == '-'
	if neg {
		n = 1
	}

	for ; n < len(b); n++ {
		if b[n] < '0' || '9' < b[n] {
			return 0, ErrMalformedInt
		}
		i = i*10 + int64(b[n]-'0')
	}

	if neg {
		i = -i
	}

	return i, nil
}

func readBulkString(size int64, r byteScanner) ([]byte, error) {
	if size < 0 {
		return nil, ErrBadSize
	}

	var buf []byte
	if size > 0 {
		buf = make([]byte, size)
		n, err := io.ReadFull(r, buf)
		if int64(n) != size && err == nil {
			err = ErrMalformedBulkString
		}
		if err != nil {
			return nil, unexpectedEOF(err)
		}
	}

	if err := readCRLF(r, ErrMalformedBulkString); err != nil {
		return nil, err
	}

	return buf, nil
}

func readArray(r byteScanner) ([]Resp, error) {
	// NOTE: Rewrite this so it's not recursive? Though the chance of that being an issue is slim.
	size, err := readInteger(r)
	if err != nil {
		return nil, err
	}

	if size == 0 {
		return nil, nil
	}

	ary := make([]Resp, size)

	for i := 0; size > 0; i, size = i+1, size-1 {
		ary[i] = Read(r)
		if ary[i].Err != nil {
			return nil, ary[i].Err
		}
	}

	return ary, nil
}

func Read(r byteScanner) (resp Resp) {
	defer func() {
		if resp.Err != nil && resp.typ != Err {
			resp.typ = Invalid
		}
	}()

	b, err := r.ReadByte()
	if err != nil {
		return Resp{Invalid, nil, unexpectedEOF(err)}
	}
	switch b {
	case '$':
		size, err := readInteger(r)
		if err != nil {
			return Resp{Invalid, nil, err}
		} else if size == -1 {
			return Resp{Nil, nil, nil}
		} else {
			s, err := readBulkString(size, r)
			if err != nil {
				return Resp{Invalid, nil, err}
			}
			return Resp{BulkStr, s, nil}
		}

	case ':':
		i, err := readInteger(r)
		return Resp{Int, i, err}

	case '-':
		inner, err := readError(r)
		if err == nil {
			err = inner
		}
		return Resp{Err, inner, err}

	case '+':
		str, err := readSimpleString(r)
		return Resp{SimpleStr, str, err}

	case '*':
		ary, err := readArray(r)
		return Resp{Array, ary, err}

	default:
		err = BadTypeError(b)
	}

	if berr := r.UnreadByte(); berr != nil {
		log.Printf("Error unreading byte: %v", berr)
	}

	return Resp{Invalid, nil, err}
}
