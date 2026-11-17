package codec

import (
	"github.com/lonng/nano/internal/packet"
	"testing"
)

func TestFragmentedSmallPacketDecodedImmediately(t *testing.T) {
	d := NewDecoder()
	b, _ := Encode(packet.Data, []byte{1, 2, 3})
	d.Decode(b[:4])
	got, err := d.Decode(b[4:])
	if err != nil || len(got) != 1 {
		t.Fatalf("complete 3-byte body: packets=%d err=%v; want 1", len(got), err)
	}
}
