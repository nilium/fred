package resv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/nilium/fred"
)

type Marshaler interface {
	MarshalRESP() (interface{}, error)
}

func MarshalRESP(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	es := encoderState{w: &buf}
	err := es.write(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type Encoder struct {
	state encoderState
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		state: encoderState{w: w},
	}
}

type encoderState struct {
	w   io.Writer
	err error
}

func (e *encoderState) write(v interface{}) (err error) {
	if e.err != nil {
		return e.err
	}

	defer func(orig interface{}) {
		if rc := recover(); rc != nil {
			if err == nil {
				err = fmt.Errorf("unable to encode %T as RESP", v)
			}

			var trace [20000]byte
			sz := runtime.Stack(trace[:], false)
			log.Printf("encoderState.write panicked: %v\ntrace:\n%s", rc, trace[:sz])
		}

		if err != nil {
			e.err = err
		}
	}(v)

	for i := 0; i < 32; i++ {
		var err error
		if m, ok := v.(Marshaler); ok {
			v, err = m.MarshalRESP()
		}

		if err != nil {
			return err
		}
	}

	if _, ok := v.(Marshaler); ok {
		return errors.New("too many recursive MarshalRESP calls")
	}

	if v == nil {
		_, err = io.WriteString(e.w, "$-1\r\n")
		return err
	}

	switch v := v.(type) {
	case error:
		msg := v.Error()
		if strings.IndexAny(msg, "\r\n") != -1 {
			return fred.ErrMalformedSimpleString
		}
		_, err = io.WriteString(e.w, "-"+msg+"\r\n")
		return err
	case string:
		_, err = fmt.Fprintf(e.w, "$%d\r\n%s\r\n", len(v), v)
		return err
	case []string:
		_, err = fmt.Fprintf(e.w, "*%d", len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			if err = e.write(s); err != nil {
				return err
			}
		}
		return err

	case []uint8:
		_, err = fmt.Fprintf(e.w, "$%d\r\n%s\r\n", len(v), v)
		return err
	case [][]uint8:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, s := range v {
			if err = e.write(s); err != nil {
				return err
			}
		}
		return err

	case int:
		if _, err = fmt.Fprintf(e.w, ":%d\r\n", v); err != nil {
			return err
		}
		return err
	case []int:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, i := range v {
			if err = e.write(i); err != nil {
				return err
			}
		}
		return err

	case int64:
		if _, err = fmt.Fprintf(e.w, ":%d\r\n", v); err != nil {
			return err
		}
		return err
	case []int64:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, i := range v {
			if err = e.write(i); err != nil {
				return err
			}
		}
		return err

	case int32:
		if _, err = fmt.Fprintf(e.w, ":%d\r\n", v); err != nil {
			return err
		}
		return err
	case []int32:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, i := range v {
			if err = e.write(i); err != nil {
				return err
			}
		}
		return err

	case int16:
		if _, err = fmt.Fprintf(e.w, ":%d\r\n", v); err != nil {
			return err
		}
		return err
	case []int16:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, i := range v {
			if err = e.write(i); err != nil {
				return err
			}
		}
		return err

	case int8:
		if _, err = fmt.Fprintf(e.w, ":%d\r\n", v); err != nil {
			return err
		}
		return err
	case []int8:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, i := range v {
			if err = e.write(i); err != nil {
				return err
			}
		}
		return err

	case uint:
		if _, err = fmt.Fprintf(e.w, ":%d\r\n", v); err != nil {
			return err
		}
		return err
	case []uint:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, i := range v {
			if err = e.write(i); err != nil {
				return err
			}
		}
		return err

	case uint64:
		if _, err = fmt.Fprintf(e.w, ":%d\r\n", v); err != nil {
			return err
		}
		return err
	case []uint64:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, i := range v {
			if err = e.write(i); err != nil {
				return err
			}
		}
		return err

	case uint32:
		if _, err = fmt.Fprintf(e.w, ":%d\r\n", v); err != nil {
			return err
		}
		return err
	case []uint32:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, i := range v {
			if err = e.write(i); err != nil {
				return err
			}
		}
		return err

	case uint16:
		if _, err = fmt.Fprintf(e.w, ":%d\r\n", v); err != nil {
			return err
		}
		return err
	case []uint16:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, i := range v {
			if err = e.write(i); err != nil {
				return err
			}
		}
		return err

	case float64:
		fls := strings.TrimRight(strconv.FormatFloat(v, 'f', 16, 64), "0.")
		if _, err = fmt.Fprintf(e.w, "$%d\r\n%s\r\n", len(fls), fls); err != nil {
			return err
		}
		return err
	case []float64:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, f := range v {
			if err = e.write(f); err != nil {
				return err
			}
		}
		return err

	case float32:
		fls := strings.TrimRight(strconv.FormatFloat(float64(v), 'f', 16, 64), "0.")
		if _, err = fmt.Fprintf(e.w, "$%d\r\n%s\r\n", len(fls), fls); err != nil {
			return err
		}
		return err
	case []float32:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, f := range v {
			if err = e.write(f); err != nil {
				return err
			}
		}
		return err

	case fred.Resp:
		value, err := v.Value()
		if err != nil {
			return err
		}
		return e.write(value)

	case time.Duration:
		ds := v.String()
		_, err = fmt.Fprintf(e.w, "$%d\r\n%s\r\n", len(ds), ds)
		return err

	case fmt.Stringer:
		s := v.String()
		_, err = fmt.Fprintf(e.w, "$%d\r\n%s\r\n", len(s), s)
		return err

	case time.Time:
		sec := v.Unix()
		nsec := v.UnixNano() - sec*int64(time.Second)
		fmt.Fprintf(e.w, "*2\r\n:%d\r\n:%d\r\n", sec, nsec)

	case []interface{}:
		_, err = fmt.Fprintf(e.w, "*%d\r\n", len(v))
		if err != nil {
			return err
		}
		for _, f := range v {
			if err = e.write(f); err != nil {
				return err
			}
		}
		return err

	}

	rv := reflect.Indirect(reflect.ValueOf(v))
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		n := rv.Len()
		vals := make([]interface{}, 0, n)
		for i := 0; i < n; i++ {
			elem := rv.Index(i)
			if !elem.CanInterface() {
				return fmt.Errorf("cannot marshal slice element %s", elem.Type().PkgPath())
			}
			vals = append(vals, elem.Interface())
		}
		e.write(vals)

	case reflect.Map:
		keys := rv.MapKeys()
		vals := make([]interface{}, 0, len(keys))
		for _, key := range keys {
			if !key.CanInterface() {
				return fmt.Errorf("cannot marshal map key %s", key.Type().PkgPath())
			}

			elem := rv.MapIndex(key)
			if !elem.CanInterface() {
				return fmt.Errorf("cannot marshal map element %s", elem.Type().PkgPath())
			}

			ik := key.Interface()
			iv := elem.Interface()
			vals = append(vals, ik, iv)
		}
		e.write(vals)

	default:
		fmt.Errorf("cannot marshal %T to RESP type")
	}

	return err
}
