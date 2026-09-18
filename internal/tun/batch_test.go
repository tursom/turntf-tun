package tun

import (
	"bytes"
	"testing"
)

func TestTunBatchRoundTrip(t *testing.T) {
	want := [][]byte{[]byte{0x45, 1, 2}, []byte{0x60, 3, 4, 5}}
	var frame []byte
	for _, packet := range want {
		var ok bool
		frame, ok = appendTunBatch(frame, packet)
		if !ok {
			t.Fatal("appendTunBatch rejected packet")
		}
	}
	var got [][]byte
	if err := decodeTunBatch(frame, 1500, func(packet []byte) error { got = append(got, append([]byte(nil), packet...)); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) || !bytes.Equal(got[0], want[0]) || !bytes.Equal(got[1], want[1]) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestTunBatchFallsBackToSinglePacket(t *testing.T) {
	packet := []byte{0x45, 1, 2}
	var got []byte
	if err := decodeTunBatch(packet, 1500, func(value []byte) error { got = value; return nil }); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, packet) {
		t.Fatalf("got %x want %x", got, packet)
	}
}
