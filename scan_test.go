package fred

import (
	"bytes"
	"log"
	"testing"
)

func init() {
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("\t")
}

func TestMapScan(t *testing.T) {
	var data map[string]int64
	msg := bytes.NewBufferString("*4\r\n+Key 1\r\n:1234\r\n$5\r\nKey 2\r\n:45678910\r\n")
	err := Scan(msg, &data)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", data)
}

func TestArrayScan(t *testing.T) {
	var data []string
	msg := bytes.NewBufferString("*4\r\n+Key 1\r\n:1234\r\n$5\r\nKey 2\r\n:45678910\r\n")
	err := Scan(msg, &data)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", data)
}

func TestNestedArrayRead(t *testing.T) {
	msg := bytes.NewBufferString("*2\r\n*2\r\n+Key 1\r\n:1234\r\n*2\r\n$5\r\nKey 2\r\n:45678910\r\n")
	resp := Read(msg)
	if resp.Err != nil {
		t.Fatal(resp.Err)
	}
	t.Logf("%#v", resp)
}

func TestArrayRead(t *testing.T) {
	msg := bytes.NewBufferString("*4\r\n+Key 1\r\n:1234\r\n$5\r\nKey 2\r\n:45678910\r\n")
	resp := Read(msg)
	if resp.Err != nil {
		t.Fatal(resp.Err)
	}
	t.Logf("%#v", resp)
}

func TestBytesList(t *testing.T) {
	{
		msg := bytes.NewBufferString("*4\r\n+Key 1\r\n$4\r\n1234\r\n$5\r\nKey 2\r\n$8\r\n45678910\r\n")
		resp := Read(msg)
		if resp.Err != nil {
			t.Error(resp.Err)
			goto next
		}

		bl, err := resp.BytesList()
		t.Logf("%#v", bl)
		if err != nil {
			t.Error("unexpected error converting string array to bytes list:", err)
		}
	}

next:
	{
		msg := bytes.NewBufferString("*4\r\n+Key 1\r\n$4\r\n1234\r\n$5\r\nKey 2\r\n:45678910\r\n")
		resp := Read(msg)
		if resp.Err != nil {
			t.Error(resp.Err)
			return
		}

		bl, err := resp.BytesList()
		t.Logf("%#v err=%v", bl, err)
		if err == nil {
			t.Errorf("expected error in converting array of mixed types to string list")
		}
	}
}

func TestStrList(t *testing.T) {
	{
		msg := bytes.NewBufferString("*4\r\n+Key 1\r\n$4\r\n1234\r\n$5\r\nKey 2\r\n$8\r\n45678910\r\n")
		resp := Read(msg)
		if resp.Err != nil {
			t.Error(resp.Err)
			goto next
		}

		bl, err := resp.StrList()
		t.Logf("%#v", bl)
		if err != nil {
			t.Error("unexpected error converting string array to string list:", err)
		}
	}

next:
	{
		msg := bytes.NewBufferString("*4\r\n+Key 1\r\n$4\r\n1234\r\n$5\r\nKey 2\r\n:45678910\r\n")
		resp := Read(msg)
		if resp.Err != nil {
			t.Error(resp.Err)
			return
		}

		bl, err := resp.StrList()
		t.Logf("%#v err=%v", bl, err)
		if err == nil {
			t.Errorf("expected error in converting array of mixed types to string list")
		}
	}
}

func TestIntScan(t *testing.T) {
	var neg int64
	var pos uint16
	msg := bytes.NewBufferString(":-123456789101\r\n:23456\r\n")
	err := Scan(msg, &neg, &pos)
	if err != nil {
		t.Fatal(err)
	}

	if pos != 23456 {
		t.Errorf("pos(%d) != 23456", pos)
	}

	if neg != -123456789101 {
		t.Errorf("neg(%d) != -123456789101", neg)
	}

	t.Logf("pos=%d neg=%d", pos, neg)
}

func TestSimpleString(t *testing.T) {
	var data string
	msg := bytes.NewBufferString("+This is a simple string\r\n+This simple string\rfails\r\n")
	err := Scan(msg, &data)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%q", data)

	err = Scan(msg, &data)
	if err != ErrMalformedSimpleString {
		t.Fatal("simple string containing \\r did not fail")
	} else {
		t.Logf("err=%v", err)
	}
	t.Logf("%q", data)

	msg = bytes.NewBufferString("+This simple string fails because of a \n\r\n")
	err = Scan(msg, &data)
	if err != ErrMalformedSimpleString {
		t.Fatal("simple string containing \\n did not fail")
	} else {
		t.Logf("err=%v", err)
	}
	t.Logf("%q", data)
}
