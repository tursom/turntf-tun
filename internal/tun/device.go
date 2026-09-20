package tun

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/songgao/water"
	"github.com/vishvananda/netlink"
)

type PacketDevice interface {
	Name() string
	ReadPacket(context.Context) ([]byte, error)
	WritePacket([]byte) error
	Close() error
}
type TUNDevice struct {
	ifce           *water.Interface
	maxPacketBytes int
	readBuf        []byte
}

func OpenTUN(cfg TunConfig, maxPacketBytes int) (*TUNDevice, error) {
	if os.Geteuid() != 0 {
		return nil, errors.New("creating tun requires root or CAP_NET_ADMIN")
	}
	ifce, err := water.New(water.Config{DeviceType: water.TUN, PlatformSpecificParams: water.PlatformSpecificParams{Name: cfg.Name}})
	if err != nil {
		return nil, fmt.Errorf("create tun: %w", err)
	}
	d := &TUNDevice{ifce: ifce, maxPacketBytes: maxPacketBytes}
	if err := configureTUN(ifce.Name(), cfg); err != nil {
		_ = d.Close()
		return nil, err
	}
	return d, nil
}
func (d *TUNDevice) Name() string { return d.ifce.Name() }
func (d *TUNDevice) ReadPacket(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	buf := d.readBuf
	if len(buf) != d.maxPacketBytes {
		buf = make([]byte, d.maxPacketBytes)
		d.readBuf = buf
	}
	n, err := d.ifce.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
func (d *TUNDevice) WritePacket(packet []byte) error { _, err := d.ifce.Write(packet); return err }
func (d *TUNDevice) Close() error                    { return d.ifce.Close() }
func configureTUNRoutes(name string, peers []PeerConfig) error {
	link, err := netlink.LinkByName(name)
	if err != nil {
		return fmt.Errorf("find tun link %s for routes: %w", name, err)
	}
	for _, peer := range peers {
		for _, raw := range peer.Routes {
			_, dst, err := net.ParseCIDR(raw)
			if err != nil {
				return fmt.Errorf("parse peer route %q: %w", raw, err)
			}
			route := &netlink.Route{LinkIndex: link.Attrs().Index, Dst: dst, Scope: netlink.SCOPE_LINK}
			if err := netlink.RouteReplace(route); err != nil {
				return fmt.Errorf("install peer route %q: %w", raw, err)
			}
		}
	}
	return nil
}

func configureTUN(name string, cfg TunConfig) error {
	link, err := netlink.LinkByName(name)
	if err != nil {
		return fmt.Errorf("find tun link %s: %w", name, err)
	}
	if cfg.MTU > 0 {
		if err := netlink.LinkSetMTU(link, cfg.MTU); err != nil {
			return fmt.Errorf("set tun mtu: %w", err)
		}
	}
	if cfg.TxQueueLength > 0 {
		if err := netlink.LinkSetTxQLen(link, cfg.TxQueueLength); err != nil {
			return fmt.Errorf("set tun tx queue length: %w", err)
		}
	}
	for _, raw := range cfg.Addresses {
		addr, err := netlink.ParseAddr(raw)
		if err != nil {
			return fmt.Errorf("parse tun address %q: %w", raw, err)
		}
		if err := netlink.AddrAdd(link, addr); err != nil && !os.IsExist(err) {
			return fmt.Errorf("add tun address %q: %w", raw, err)
		}
	}
	if cfg.BringUp {
		if err := netlink.LinkSetUp(link); err != nil {
			return fmt.Errorf("bring tun up: %w", err)
		}
	}
	return nil
}
