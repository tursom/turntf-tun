package tun

import (
	"fmt"
	"net"
	"sort"
)

type route struct {
	prefix net.IPNet
	peer   *peerConfig
}
type routeTable struct{ routes []route }

func newRouteTable(peers []PeerConfig) (*routeTable, error) {
	t := &routeTable{}
	for i := range peers {
		p := &peers[i]
		for _, raw := range p.Routes {
			_, n, err := net.ParseCIDR(raw)
			if err != nil {
				return nil, fmt.Errorf("peer %s route %q: %w", p.Name, raw, err)
			}
			t.routes = append(t.routes, route{prefix: *n, peer: &peerConfig{config: p}})
		}
	}
	sort.SliceStable(t.routes, func(i, j int) bool {
		a, _ := t.routes[i].prefix.Mask.Size()
		b, _ := t.routes[j].prefix.Mask.Size()
		return a > b
	})
	return t, nil
}
func (t *routeTable) lookup(packet []byte) *peerConfig {
	ip := packetDestination(packet)
	if ip == nil {
		return nil
	}
	for _, r := range t.routes {
		if r.prefix.Contains(ip) {
			return r.peer
		}
	}
	return nil
}
func packetDestination(packet []byte) net.IP {
	if len(packet) < 1 {
		return nil
	}
	switch packet[0] >> 4 {
	case 4:
		if len(packet) < 20 {
			return nil
		}
		return net.IPv4(packet[16], packet[17], packet[18], packet[19])
	case 6:
		if len(packet) < 40 {
			return nil
		}
		return net.IP(packet[24:40])
	}
	return nil
}
