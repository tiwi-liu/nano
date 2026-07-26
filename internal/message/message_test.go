package message

import (
	"reflect"
	"testing"
)

func TestEncode(t *testing.T) {
	dict := map[string]uint16{
		"test.test.test":  100,
		"test.test.test1": 101,
		"test.test.test2": 102,
		"test.test.test3": 103,
	}
	SetDictionary(dict)
	m1 := &Message{
		Type:       Request,
		ID:         100,
		Route:      "test.test.test",
		Data:       []byte(`hello world`),
		compressed: true,
	}
	em1, err := m1.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm1, err := Decode(em1)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m1, dm1) {
		t.Error("not equal")
	}

	m2 := &Message{
		Type:  Request,
		ID:    100,
		Route: "test.test.test4",
		Data:  []byte(`hello world`),
	}
	em2, err := m2.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm2, err := Decode(em2)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m2, dm2) {
		t.Error("not equal")
	}

	m3 := &Message{
		Type: Response,
		ID:   100,
		Data: []byte(`hello world`),
	}
	em3, err := m3.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm3, err := Decode(em3)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m3, dm3) {
		t.Error("not equal")
	}

	m4 := &Message{
		Type: Response,
		ID:   100,
		Data: []byte(`hello world`),
	}
	em4, err := m4.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm4, err := Decode(em4)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m4, dm4) {
		t.Error("not equal")
	}

	m5 := &Message{
		Type:       Notify,
		Route:      "test.test.test",
		Data:       []byte(`hello world`),
		compressed: true,
	}
	em5, err := m5.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm5, err := Decode(em5)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m5, dm5) {
		t.Error("not equal")
	}

	m6 := &Message{
		Type:  Notify,
		Route: "test.test.test20",
		Data:  []byte(`hello world`),
	}
	em6, err := m6.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm6, err := Decode(em6)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m6, dm6) {
		t.Error("not equal")
	}

	m7 := &Message{
		Type:  Push,
		Route: "test.test.test9",
		Data:  []byte(`hello world`),
	}
	em7, err := m7.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm7, err := Decode(em7)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m7, dm7) {
		t.Error("not equal")
	}

	m8 := &Message{
		Type:       Push,
		Route:      "test.test.test3",
		Data:       []byte(`hello world`),
		compressed: true,
	}
	em8, err := m8.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm8, err := Decode(em8)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m8, dm8) {
		t.Error("not equal")
	}
}

func TestDecodeRejectsMalformedCompressedRoute(t *testing.T) {
	// Notify + compressed route flag, but no two-byte route code follows.
	if _, err := Decode([]byte{byte(Notify<<1) | msgRouteCompressMask, 0x01}); err == nil {
		t.Fatal("malformed compressed route should fail")
	}
}

func TestDecodeRejectsUnterminatedMessageID(t *testing.T) {
	data := []byte{byte(Request << 1)}
	for i := 0; i < 11; i++ {
		data = append(data, 0x80)
	}
	if _, err := Decode(data); err == nil {
		t.Fatal("unterminated message id should fail")
	}
}

func TestEncodeRejectsOversizedRoute(t *testing.T) {
	m := &Message{Type: Notify, Route: string(make([]byte, msgRouteLengthMask+1))}
	if _, err := Encode(m); err == nil {
		t.Fatal("oversized route should fail")
	}
}
