package tun

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	turntf "github.com/tursom/turntf-go"
)

const relaySendBufferBytes = 8 << 20
const relayWindowSize = 32

type Logger interface{ Printf(string, ...any) }

type peerConfig struct{ config *PeerConfig }
type peerPort struct {
	peer  *peerConfig
	conn  *turntf.RelayConnection
	queue chan []byte
	done  chan struct{}
	once  sync.Once
}

func (p *peerPort) close() {
	p.once.Do(func() { close(p.done); p.conn.Abort(errors.New("replaced or closed")) })
}
func (p *peerPort) enqueue(packet []byte) bool {
	packet = append([]byte(nil), packet...)
	select {
	case p.queue <- packet:
		return true
	default:
		return false
	}
}

type Runtime struct {
	cfg      Config
	logger   Logger
	device   PacketDevice
	client   *turntf.Client
	relay    *turntf.Relay
	relayCfg turntf.RelayConfig
	routes   *routeTable
	mu       sync.RWMutex
	ports    map[turntf.UserRef]*peerPort
	writeMu  sync.Mutex
}

func Run(ctx context.Context, cfg Config, logger Logger) error {
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	routes, err := newRouteTable(cfg.Peers)
	if err != nil {
		return err
	}
	device, err := OpenTUN(cfg.Tun, cfg.Transport.MaxPacketBytes)
	if err != nil {
		return err
	}
	defer device.Close()
	rt, err := NewRuntime(cfg, device, routes, logger)
	if err != nil {
		return err
	}
	if err := configureTUNRoutes(device.Name(), cfg.Peers); err != nil {
		_ = device.Close()
		return err
	}
	return rt.Run(ctx)
}
func NewRuntime(cfg Config, device PacketDevice, routes *routeTable, logger Logger) (*Runtime, error) {
	credentials, err := cfg.Turntf.Credentials.ToTurntf()
	if err != nil {
		return nil, err
	}
	client, err := turntf.NewClient(turntf.Config{BaseURL: cfg.Turntf.BaseURL, Credentials: credentials, CursorStore: turntf.NewMemoryCursorStore(), Handler: turntf.NopHandler{}, RequestTimeout: cfg.Turntf.RequestTimeout.Duration, PingInterval: cfg.Turntf.PingInterval.Duration, TransientOnly: true, RealtimeStream: true})
	if err != nil {
		return nil, err
	}
	relayCfg := turntf.DefaultRelayConfig()
	relayCfg.Reliability = turntf.ReliabilityAtLeastOnce
	relayCfg.DeliveryMode = turntf.DeliveryModeBestEffort
	relayCfg.WindowSize = relayWindowSize
	relayCfg.SendBufferSize = relaySendBufferBytes
	return &Runtime{cfg: cfg, logger: logger, device: device, client: client, relay: client.Relay(), relayCfg: relayCfg, routes: routes, ports: make(map[turntf.UserRef]*peerPort)}, nil
}
func (r *Runtime) Run(ctx context.Context) error {
	defer r.client.Close()
	r.relay.SetIncomingConfig(r.relayCfg)
	r.relay.OnConnection(func(conn *turntf.RelayConnection) { r.acceptRelay(ctx, conn) })
	if err := r.client.Connect(ctx); err != nil {
		return err
	}
	login, ok := r.client.CurrentLogin()
	if !ok {
		return errors.New("turntf client connected without login state")
	}
	r.logf("connected as %d:%d", login.User.NodeID, login.User.UserID)
	localUser := turntf.UserRef{NodeID: login.User.NodeID, UserID: login.User.UserID}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); r.readTUNLoop(ctx) }()
	for i := range r.cfg.Peers {
		p := r.cfg.Peers[i]
		if shouldDial(localUser, p.User.ToTurntf(), p.DialPolicy) {
			wg.Add(1)
			go func() { defer wg.Done(); r.dialLoop(ctx, p) }()
		}
	}
	<-ctx.Done()
	_ = r.device.Close()
	r.closePorts()
	wg.Wait()
	return nil
}
func (r *Runtime) readTUNLoop(ctx context.Context) {
	for ctx.Err() == nil {
		packet, err := r.device.ReadPacket(ctx)
		if err != nil {
			if ctx.Err() == nil {
				r.logf("read tun: %v", err)
			}
			return
		}
		if len(packet) == 0 || len(packet) > r.cfg.Transport.MaxPacketBytes {
			continue
		}
		peer := r.routes.lookup(packet)
		if peer == nil {
			continue
		}
		r.mu.RLock()
		port := r.ports[peer.config.User.ToTurntf()]
		r.mu.RUnlock()
		if port != nil {
			port.enqueue(packet)
		}
	}
}
func (r *Runtime) dialLoop(ctx context.Context, peer PeerConfig) {
	target := peer.User.ToTurntf()
	for ctx.Err() == nil {
		conn, err := r.relay.Connect(ctx, target, &r.relayCfg)
		if err != nil {
			r.logf("connect peer %s: %v", peer.Name, err)
			if !sleepContext(ctx, r.cfg.Transport.DialRetryInterval.Duration) {
				return
			}
			continue
		}
		r.logf("connected peer %s relay=%s", peer.Name, conn.RelayID())
		port := r.registerPort(ctx, peer, conn)
		<-port.done
		if !sleepContext(ctx, r.cfg.Transport.DialRetryInterval.Duration) {
			return
		}
	}
}
func (r *Runtime) acceptRelay(ctx context.Context, conn *turntf.RelayConnection) {
	remote := conn.RemotePeer()
	var peer *PeerConfig
	for i := range r.cfg.Peers {
		if r.cfg.Peers[i].User.ToTurntf() == remote {
			peer = &r.cfg.Peers[i]
			break
		}
	}
	if peer == nil {
		conn.Abort(fmt.Errorf("unknown peer %d:%d", remote.NodeID, remote.UserID))
		return
	}
	r.logf("accepted peer %s relay=%s", peer.Name, conn.RelayID())
	r.registerPort(ctx, *peer, conn)
}
func (r *Runtime) registerPort(ctx context.Context, peer PeerConfig, conn *turntf.RelayConnection) *peerPort {
	p := &peerPort{peer: &peerConfig{config: &peer}, conn: conn, queue: make(chan []byte, r.cfg.Transport.SendQueueSize), done: make(chan struct{})}
	user := peer.User.ToTurntf()
	r.mu.Lock()
	old := r.ports[user]
	r.ports[user] = p
	r.mu.Unlock()
	if old != nil {
		old.close()
	}
	go r.writeRelayLoop(p)
	go r.readRelayLoop(ctx, p)
	conn.OnClose(func(error) {
		r.mu.Lock()
		if r.ports[user] == p {
			delete(r.ports, user)
		}
		r.mu.Unlock()
		p.once.Do(func() { close(p.done) })
	})
	return p
}
func (r *Runtime) writeRelayLoop(p *peerPort) {
	var pending []byte
	for {
		if pending == nil {
			select {
			case <-p.done:
				return
			case pending = <-p.queue:
			}
		}
		batch, ok := appendTunBatch(nil, pending)
		if !ok {
			r.logf("drop oversized tun packet for peer %s", p.peer.config.Name)
			pending = nil
			continue
		}
		pending = nil
		timer := time.NewTimer(time.Millisecond)
	collect:
		for len(batch) < tunBatchMaxBytes {
			select {
			case <-p.done:
				timer.Stop()
				return
			case packet := <-p.queue:
				var added bool
				batch, added = appendTunBatch(batch, packet)
				if !added {
					pending = packet
					break collect
				}
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if err := p.conn.Send(batch); err != nil {
			r.logf("send peer %s: %v", p.peer.config.Name, err)
			p.close()
			return
		}
	}
}
func (r *Runtime) readRelayLoop(ctx context.Context, p *peerPort) {
	for {
		frame, err := p.conn.ReceiveTimeout(time.Second)
		if err != nil {
			var re *turntf.RelayError
			if errors.As(err, &re) && re.Code == turntf.RelayErrorReceiveTimeout {
				continue
			}
			p.close()
			return
		}
		err = decodeTunBatch(frame, r.cfg.Transport.MaxPacketBytes, func(packet []byte) error {
			r.writeMu.Lock()
			defer r.writeMu.Unlock()
			return r.device.WritePacket(packet)
		})
		if err != nil {
			if ctx.Err() == nil {
				r.logf("write tun from %s: %v", p.peer.config.Name, err)
			}
			p.close()
			return
		}
	}
}
func (r *Runtime) closePorts() {
	r.mu.Lock()
	ports := make([]*peerPort, 0, len(r.ports))
	for _, p := range r.ports {
		ports = append(ports, p)
	}
	r.mu.Unlock()
	for _, p := range ports {
		p.close()
	}
}
func shouldDial(local, remote turntf.UserRef, policy string) bool {
	switch policy {
	case "always":
		return true
	case "never":
		return false
	default:
		if local.NodeID != remote.NodeID {
			return local.NodeID < remote.NodeID
		}
		return local.UserID < remote.UserID
	}
}
func sleepContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (r *Runtime) logf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}
