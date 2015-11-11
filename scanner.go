package fred

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrArrayLength = errors.New("Scan: array length is less than response length")
	ErrMapLength   = errors.New("Scan: response length is odd")
	errWrongType   = errors.New("Scan: unable to convert type for scanning")
)

var bytesPtrType = reflect.TypeOf((*[]byte)(nil))

type TypeError struct {
	From, To reflect.Type
}

func (t TypeError) Error() string {
	return fmt.Sprintf("Scan: unable to coerce %v to %v", t.From, t.To)
}

// Marshaler is responsible for coercing the marshal-able type to a primitive value that can be marshalled for use
// as a RESP message. Acceptable results are nil, a slice of any primitives, integers, floats, bools, strings, and byte
// slices. Any other type will produce an error on marshalling. Strings and byte slices are both considered bulk strings.
// type Marshaler interface {
// 	MarshalResp() (interface{}, error)
// }

type Unmarshaler interface {
	UnmarshalResp(Resp) error
}

func scan(dst interface{}, resp Resp) (err error) {
	src := resp.value
	var outer reflect.Value
	if rst, ok := dst.(reflect.Value); ok {
		outer = rst
	} else {
		outer = reflect.ValueOf(dst)
	}

	defer func() {
		if rc := recover(); rc != nil {
			err = errWrongType
		}

		if err == errWrongType {
			err = TypeError{To: outer.Type(), From: reflect.TypeOf(src)}
		}
	}()

	if s, ok := dst.(Unmarshaler); ok {
		return s.UnmarshalResp(resp)
	}

	if outer.Kind() != reflect.Ptr {
		return errWrongType
	}

	val := reflect.Indirect(outer)

	switch val.Kind() {
	case reflect.Bool:
		if i, err := toInt(src, 64); err == nil {
			val.SetBool(i != 0)
		} else {
			return err
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if i, err := toInt(src, val.Type().Bits()); err == nil {
			val.SetInt(i)
		} else {
			return err
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if i, err := toUint(src, val.Type().Bits()); err == nil {
			val.SetUint(i)
		} else {
			return err
		}
	case reflect.Float32, reflect.Float64:
		if f, err := toFloat(src, val.Type().Bits()); err == nil {
			val.SetFloat(f)
		} else {
			return err
		}
	case reflect.String:
		if s, err := toString(src); err == nil {
			val.SetString(s)
		} else {
			return err
		}
	case reflect.Slice:
		if bytesPtrType.AssignableTo(outer.Type()) {
			bs, err := resp.Bytes()
			if err == nil {
				val.Set(reflect.ValueOf(bs))
			}
			return err
		}

		ary, ok := src.([]Resp)
		if !ok {
			return errWrongType
		} else if len(ary) == 0 {
			outer.Set(reflect.Zero(val.Type()))
			return nil
		}
		val = reflect.MakeSlice(val.Type(), len(ary), len(ary))
		for i, obj := range ary {
			if err := scan(val.Index(i).Addr(), obj); err != nil {
				return err
			}
		}
		reflect.Indirect(outer).Set(val)

	case reflect.Array:
		ary, ok := src.([]Resp)
		if !ok {
			return errWrongType
		} else if len(ary) == 0 {
			outer.Set(reflect.Zero(val.Type()))
			return nil
		} else if len(ary) > val.Len() {
			return ErrArrayLength
		}

		for i, obj := range ary {
			if err := scan(val.Index(i).Addr(), obj); err != nil {
				return err
			}
		}

		zero := reflect.Zero(val.Type().Elem())
		for i := len(ary); i < val.Type().Len(); i++ {
			val.Index(i).Set(zero)
		}

	case reflect.Map:
		ary, ok := src.([]Resp)
		if !ok {
			return errWrongType
		} else if len(ary) == 0 {
			outer.Set(reflect.Zero(val.Type()))
			return nil
		} else if len(ary)&1 == 1 {
			return ErrMapLength
		}

		val = reflect.MakeMap(val.Type())
		kt, et := val.Type().Key(), val.Type().Elem()

		i := 0
		for ; i < len(ary); i += 2 {
			// Key
			zk := reflect.New(kt)
			if err := scan(zk, ary[i]); err != nil {
				return err
			}

			// Element
			ze := reflect.New(et)
			if err := scan(ze, ary[i+1]); err != nil {
				return err
			}

			val.SetMapIndex(reflect.Indirect(zk), reflect.Indirect(ze))
		}
		reflect.Indirect(outer).Set(val)

	default:
		return errWrongType
	}

	return nil
}

func Scan(r byteScanner, dst ...interface{}) error {
	for _, target := range dst {
		val := Read(r)
		err := val.Err
		if err == nil {
			err = scan(target, val)
		}

		if err != nil {
			return err
		}
	}
	return nil
}
