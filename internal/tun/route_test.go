package tun

import "testing"

func TestRouteTableUsesLongestPrefix(t *testing.T) {
	peers := []PeerConfig{
		{Name: "network", User: UserRefConfig{NodeID: 1, UserID: 1}, Routes: []string{"10.250.0.0/24"}},
		{Name: "host", User: UserRefConfig{NodeID: 1, UserID: 2}, Routes: []string{"10.250.0.3/32"}},
	}
	table, err := newRouteTable(peers)
	if err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 20)
	packet[0] = 0x45
	copy(packet[16:20], []byte{10, 250, 0, 3})
	if got := table.lookup(packet); got == nil || got.config.Name != "host" {
		t.Fatalf("host route = %#v", got)
	}
	copy(packet[16:20], []byte{10, 250, 0, 9})
	if got := table.lookup(packet); got == nil || got.config.Name != "network" {
		t.Fatalf("network route = %#v", got)
	}
}

func TestPacketDestinationSupportsIPv4AndIPv6(t *testing.T) {
	v4 := make([]byte, 20)
	v4[0] = 0x45
	copy(v4[16:20], []byte{192, 0, 2, 1})
	if got := packetDestination(v4).String(); got != "192.0.2.1" {
		t.Fatal(got)
	}
	v6 := make([]byte, 40)
	v6[0] = 0x60
	v6[24] = 0x20
	v6[25] = 0x01
	v6[39] = 1
	if got := packetDestination(v6).String(); got != "2001::1" {
		t.Fatal(got)
	}
}
