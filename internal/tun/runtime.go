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

type Logger interface{ Printf(string, ...any) }

type peerConfig struct{ config *PeerConfig }

type relayConn interface {
	RelayID() string
	Send([]byte) error
	ReceiveTimeout(time.Duration) ([]byte, error)
	Abort(error)
	OnClose(func(error))
}

type peerPort struct {
	runtime      *Runtime
	peer         *peerConfig
	conn         relayConn
	queue        chan []byte
	done         chan struct{}
	doneOnce     sync.Once
	abortOnce    sync.Once
	queuedOnce   sync.Once
	sentOnce     sync.Once
	receivedOnce sync.Once
}

func (p *peerPort) closeDone() {
	p.doneOnce.Do(func() { close(p.done) })
}

func (p *peerPort) close() {
	p.closeDone()
	p.abortOnce.Do(func() {
		if p.conn != nil {
			p.conn.Abort(errors.New("replaced or closed"))
		}
	})
}
func (p *peerPort) enqueue(packet []byte) bool {
	packet = append([]byte(nil), packet...)
	select {
	case p.queue <- packet:
		p.queuedOnce.Do(func() {
			if p.runtime != nil && p.peer != nil && p.peer.config != nil {
				p.runtime.logf("relay packet queued for peer %s", p.peer.config.Name)
			}
		})
		return true
	default:
		return false
	}
}

type Runtime struct {
	cfg              Config
	logger           Logger
	device           PacketDevice
	client           *turntf.Client
	relay            *turntf.Relay
	relayCfg         turntf.RelayConfig
	routes           *routeTable
	mu               sync.RWMutex
	ports            map[turntf.UserRef]*peerPort
	relayPorts       map[turntf.UserRef]map[string]*peerPort
	activeStreams    map[turntf.UserRef]*streamPort
	writeMu          sync.Mutex
	streamMu         sync.RWMutex
	streamPorts      map[turntf.UserRef]*streamPort
	streamRecv       map[turntf.StreamID]*streamReceiver
	preferredStreams map[turntf.UserRef]turntf.SessionRef
	streamResolve    func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error)
	streamSend       func(context.Context, turntf.UserRef, turntf.SessionRef, turntf.StreamFrame, turntf.DeliveryMode) (turntf.RelayAccepted, error)
	streamNewID      func() (turntf.StreamID, error)
	streamTimeout    time.Duration
	relayDial        func(context.Context, PeerConfig)
	localUser        turntf.UserRef
	connected        bool
	releaseLease     func(context.Context)
}

func Run(ctx context.Context, cfg Config, logger Logger) error {
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	rt, err := NewRuntime(cfg, nil, nil, logger)
	if err != nil {
		return err
	}
	if err := rt.client.Connect(ctx); err != nil {
		return err
	}
	login, ok := rt.client.CurrentLogin()
	if !ok {
		return errors.New("turntf client connected without login state")
	}
	if cfg.Overlay.Enabled {
		kvClient := newKVHTTPClient(cfg.Turntf)
		if err := kvClient.login(ctx, cfg.Turntf.Credentials); err != nil {
			return err
		}
		lease, err := kvClient.acquire(ctx, cfg.Overlay, turntf.UserRef{NodeID: login.User.NodeID, UserID: login.User.UserID})
		if err != nil {
			return err
		}
		cfg.Tun.Addresses = []string{lease.Address}
		go renewOverlayLease(ctx, kvClient, cfg.Overlay, lease, turntf.UserRef{NodeID: login.User.NodeID, UserID: login.User.UserID})
		cfg.Peers, err = discoverOverlayPeers(ctx, kvClient, cfg.Overlay, turntf.UserRef{NodeID: login.User.NodeID, UserID: login.User.UserID}, cfg.Peers)
		if err != nil {
			return err
		}
		rt.releaseLease = func(releaseCtx context.Context) { _ = kvClient.releaseLease(releaseCtx, cfg.Overlay.Database, lease) }
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
	rt.cfg, rt.routes, rt.device, rt.connected = cfg, routes, device, true
	if err := configureTUNRoutes(device.Name(), cfg.Peers); err != nil {
		return err
	}
	if rt.releaseLease != nil {
		defer rt.releaseLease(context.Background())
	}
	return rt.Run(ctx)
}
func NewRuntime(cfg Config, device PacketDevice, routes *routeTable, logger Logger) (*Runtime, error) {
	credentials, err := cfg.Turntf.Credentials.ToTurntf()
	if err != nil {
		return nil, err
	}
	relayCfg := turntf.DefaultRelayConfig()
	relayCfg.Reliability = turntf.ReliabilityAtLeastOnce
	relayCfg.DeliveryMode = turntf.DeliveryModeBestEffort
	relayCfg.WindowSize = cfg.Transport.RelayWindowSize
	relayCfg.SendBufferSize = relaySendBufferBytes
	rt := &Runtime{cfg: cfg, logger: logger, device: device, routes: routes, relayCfg: relayCfg, ports: make(map[turntf.UserRef]*peerPort), relayPorts: make(map[turntf.UserRef]map[string]*peerPort), activeStreams: make(map[turntf.UserRef]*streamPort), streamPorts: make(map[turntf.UserRef]*streamPort), streamRecv: make(map[turntf.StreamID]*streamReceiver), preferredStreams: make(map[turntf.UserRef]turntf.SessionRef)}
	client, err := turntf.NewClient(turntf.Config{BaseURL: cfg.Turntf.BaseURL, Credentials: credentials, CursorStore: turntf.NewMemoryCursorStore(), Handler: runtimeHandler{runtime: rt}, RequestTimeout: cfg.Turntf.RequestTimeout.Duration, PingInterval: cfg.Turntf.PingInterval.Duration, TransientOnly: true, RealtimeStream: true})
	if err != nil {
		return nil, err
	}
	rt.client = client
	rt.relay = client.Relay()
	return rt, nil
}
func (r *Runtime) Run(ctx context.Context) error {
	defer r.client.Close()
	r.relay.SetIncomingConfig(r.relayCfg)
	r.relay.OnConnection(func(conn *turntf.RelayConnection) { r.acceptRelay(ctx, conn) })
	if !r.connected {
		if err := r.client.Connect(ctx); err != nil {
			return err
		}
	}
	login, ok := r.client.CurrentLogin()
	if !ok {
		return errors.New("turntf client connected without login state")
	}
	r.logf("connected as %d:%d", login.User.NodeID, login.User.UserID)
	localUser := turntf.UserRef{NodeID: login.User.NodeID, UserID: login.User.UserID}
	r.localUser = localUser
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); r.readTUNLoop(ctx) }()
	for i := range r.cfg.Peers {
		p := r.cfg.Peers[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.runPeer(ctx, localUser, p)
		}()
	}
	<-ctx.Done()
	_ = r.device.Close()
	r.closePorts()
	wg.Wait()
	return nil
}

func (r *Runtime) runPeer(ctx context.Context, localUser turntf.UserRef, peer PeerConfig) {
	if peer.effectiveTransportMode(r.cfg.Transport.Mode) == "stream" {
		// Stream offsets are unidirectional. Each peer owns one outbound
		// logical stream so TUN request and response packets have independent
		// ACK/window/Resume state.
		r.streamLoop(ctx, peer)
		return
	}
	if shouldDial(localUser, peer.User.ToTurntf(), peer.DialPolicy) {
		r.dialPeer(ctx, peer)
	}
}

func (r *Runtime) dialPeer(ctx context.Context, peer PeerConfig) {
	if r.relayDial != nil {
		r.relayDial(ctx, peer)
		return
	}
	r.dialLoop(ctx, peer)
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
		user := peer.config.User.ToTurntf()
		r.mu.RLock()
		stream := r.activeStreams[user]
		port := r.ports[user]
		r.mu.RUnlock()
		if stream != nil {
			stream.enqueue(packet)
			continue
		}
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
func (r *Runtime) registerPort(ctx context.Context, peer PeerConfig, conn relayConn) *peerPort {
	p := &peerPort{runtime: r, peer: &peerConfig{config: &peer}, conn: conn, queue: make(chan []byte, r.cfg.Transport.SendQueueSize), done: make(chan struct{})}
	user := peer.User.ToTurntf()
	r.mu.Lock()
	if r.relayPorts == nil {
		r.relayPorts = make(map[turntf.UserRef]map[string]*peerPort)
	}
	connections := r.relayPorts[user]
	if connections == nil {
		connections = make(map[string]*peerPort)
		r.relayPorts[user] = connections
	}
	if connections[conn.RelayID()] != nil {
		r.mu.Unlock()
		p.close()
		return p
	}
	connections[conn.RelayID()] = p
	selected := r.ports[user]
	if selected == nil || conn.RelayID() < selected.conn.RelayID() {
		r.ports[user] = p
	}
	r.mu.Unlock()

	conn.OnClose(func(error) { r.unregisterPort(user, p) })
	go r.writeRelayLoop(p)
	go r.readRelayLoop(ctx, p)
	return p
}

// unregisterPort removes only the connection that closed. Non-selected
// connections remain receive-capable and the lowest live relay ID becomes the
// next outbound path without aborting any other session's path.
func (r *Runtime) unregisterPort(user turntf.UserRef, p *peerPort) {
	r.mu.Lock()
	connections := r.relayPorts[user]
	if connections[p.conn.RelayID()] == p {
		delete(connections, p.conn.RelayID())
	}
	if len(connections) == 0 {
		delete(r.relayPorts, user)
	}
	if r.ports[user] == p {
		delete(r.ports, user)
		for _, candidate := range connections {
			selected := r.ports[user]
			if selected == nil || candidate.conn.RelayID() < selected.conn.RelayID() {
				r.ports[user] = candidate
			}
		}
	}
	r.mu.Unlock()
	p.closeDone()
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
		p.sentOnce.Do(func() { p.runtime.logf("relay data sent to peer %s", p.peer.config.Name) })
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
		p.receivedOnce.Do(func() { r.logf("relay data received from peer %s", p.peer.config.Name) })
	}
}
func (r *Runtime) closePorts() {
	r.mu.Lock()
	seen := make(map[*peerPort]struct{})
	var ports []*peerPort
	for _, connections := range r.relayPorts {
		for _, p := range connections {
			seen[p] = struct{}{}
			ports = append(ports, p)
		}
	}
	for _, p := range r.ports {
		if _, ok := seen[p]; !ok {
			ports = append(ports, p)
		}
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
func (r *Runtime) logRelayOrphan(relayID string, kind turntf.RelayKind) {
	r.logf("orphan relay frame relay_id=%q kind=%s", relayID, relayKindName(kind))
}

func relayKindName(kind turntf.RelayKind) string {
	switch kind {
	case turntf.RelayKindOpen:
		return "OPEN"
	case turntf.RelayKindOpenAck:
		return "OPEN_ACK"
	case turntf.RelayKindData:
		return "DATA"
	case turntf.RelayKindAck:
		return "ACK"
	case turntf.RelayKindClose:
		return "CLOSE"
	case turntf.RelayKindPing:
		return "PING"
	case turntf.RelayKindError:
		return "ERROR"
	default:
		return "UNSPECIFIED"
	}
}

func (r *Runtime) logf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}
