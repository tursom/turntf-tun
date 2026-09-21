package tun

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	turntf "github.com/tursom/turntf-go"
)

func TestRelayOrphanLogContainsOnlySafeMetadata(t *testing.T) {
	logger := &recordingLogger{}
	r := &Runtime{logger: logger}
	r.logRelayOrphan("relay\nsecret", turntf.RelayKindData)
	line := logger.last()
	if !strings.Contains(line, `relay_id="relay\nsecret"`) || !strings.Contains(line, "kind=DATA") {
		t.Fatalf("orphan log = %q", line)
	}
	if strings.Contains(line, "payload") || strings.Contains(line, "credential") {
		t.Fatalf("orphan log exposed forbidden fields: %q", line)
	}
}

func TestRunPeerMixedTransportOnlyStreamPeerSendsOpen(t *testing.T) {
	local := turntf.UserRef{NodeID: 1, UserID: 1}
	streamRef := turntf.UserRef{NodeID: 2, UserID: 1}
	relayRef := turntf.UserRef{NodeID: 3, UserID: 1}
	streamPeer := PeerConfig{Name: "stream", User: UserRefConfig{NodeID: streamRef.NodeID, UserID: streamRef.UserID}}
	relayPeer := PeerConfig{Name: "relay", User: UserRefConfig{NodeID: relayRef.NodeID, UserID: relayRef.UserID}, TransportMode: "relay"}
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "session"}
	opens := make(chan turntf.UserRef, 32)
	dials := make(chan string, 4)
	r := &Runtime{
		cfg:           Config{Transport: TransportConfig{Mode: "stream", SendQueueSize: 4, DialRetryInterval: Duration{Duration: time.Millisecond}}},
		ports:         make(map[turntf.UserRef]*peerPort),
		activeStreams: make(map[turntf.UserRef]*streamPort),
		streamPorts:   make(map[turntf.UserRef]*streamPort),
		streamRecv:    make(map[turntf.StreamID]*streamReceiver),
		localUser:     local,
		streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{{Session: session, TransientCapable: true}}}, nil
		},
		streamNewID: func() (turntf.StreamID, error) { return turntf.NewStreamID() },
		streamSend: func(_ context.Context, peer turntf.UserRef, _ turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			if frame.Kind == turntf.StreamFrameOpen {
				opens <- peer
			}
			return turntf.RelayAccepted{}, errors.New("force fallback")
		},
		relayDial: func(ctx context.Context, peer PeerConfig) {
			dials <- peer.Name
			<-ctx.Done()
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, peer := range []PeerConfig{streamPeer, relayPeer} {
		wg.Add(1)
		go func(peer PeerConfig) {
			defer wg.Done()
			r.runPeer(ctx, local, peer)
		}(peer)
	}

	select {
	case peer := <-opens:
		if peer != streamRef {
			t.Fatalf("StreamOpen target = %v, want %v", peer, streamRef)
		}
	case <-time.After(time.Second):
		t.Fatal("stream peer did not send StreamOpen")
	}
	waitForNamedDial(t, dials, relayPeer.Name)
	time.Sleep(10 * time.Millisecond)
	for {
		select {
		case peer := <-opens:
			if peer != streamRef {
				t.Fatalf("StreamOpen target = %v, want only %v", peer, streamRef)
			}
		default:
			cancel()
			waitForGroup(t, &wg)
			return
		}
	}
}

func TestReadTUNUsesActiveStreamPerPeerAndRelayForOthers(t *testing.T) {
	streamRef := turntf.UserRef{NodeID: 2, UserID: 1}
	relayRef := turntf.UserRef{NodeID: 3, UserID: 1}
	peers := []PeerConfig{
		{Name: "stream", User: UserRefConfig{NodeID: streamRef.NodeID, UserID: streamRef.UserID}, Routes: []string{"10.0.0.2/32"}},
		{Name: "relay", User: UserRefConfig{NodeID: relayRef.NodeID, UserID: relayRef.UserID}, Routes: []string{"10.0.0.3/32"}},
	}
	routes, err := newRouteTable(peers)
	if err != nil {
		t.Fatal(err)
	}
	streamQueue := make(chan []byte, 1)
	relayQueue := make(chan []byte, 1)
	r := &Runtime{
		cfg: Config{Transport: TransportConfig{MaxPacketBytes: 65535}},
		device: &queuedPacketDevice{packets: [][]byte{
			ipv4Packet(10, 0, 0, 2),
			ipv4Packet(10, 0, 0, 3),
		}},
		routes:        routes,
		ports:         map[turntf.UserRef]*peerPort{relayRef: {queue: relayQueue}},
		activeStreams: map[turntf.UserRef]*streamPort{streamRef: {queue: streamQueue}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.readTUNLoop(ctx)
		close(done)
	}()
	select {
	case packet := <-streamQueue:
		if got := packetDestination(packet).String(); got != "10.0.0.2" {
			t.Fatalf("stream packet destination = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("stream peer packet was not queued to active stream")
	}
	select {
	case packet := <-relayQueue:
		if got := packetDestination(packet).String(); got != "10.0.0.3" {
			t.Fatalf("relay packet destination = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("relay peer packet was not queued to Relay")
	}
	cancel()
	waitForDone(t, done, "TUN read loop did not stop")
}

func TestReadRelayLoopWritesDecodedBatchToDevice(t *testing.T) {
	device := &recordingPacketDevice{writes: make(chan []byte, 2)}
	conn := newFakeRelayConn("relay-a")
	r := &Runtime{
		cfg:    Config{Transport: TransportConfig{MaxPacketBytes: 65535}},
		device: device,
	}
	peer := PeerConfig{Name: "peer"}
	port := &peerPort{runtime: r, peer: &peerConfig{config: &peer}, conn: conn, done: make(chan struct{})}
	first := ipv4Packet(10, 0, 0, 2)
	second := ipv4Packet(10, 0, 0, 3)
	batch, ok := appendTunBatch(nil, first)
	if !ok {
		t.Fatal("append first packet")
	}
	batch, ok = appendTunBatch(batch, second)
	if !ok {
		t.Fatal("append second packet")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		r.readRelayLoop(ctx, port)
		close(done)
	}()
	conn.receive <- batch

	for _, want := range [][]byte{first, second} {
		select {
		case got := <-device.writes:
			if string(got) != string(want) {
				t.Fatalf("device packet = %v, want %v", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("relay batch was not written to device")
		}
	}
	port.close()
	waitForDone(t, done, "relay read loop did not stop")
}

func TestClosePortAllowsSynchronousOnCloseReentry(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 2, UserID: 1}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	r := &Runtime{
		cfg:        Config{Transport: TransportConfig{SendQueueSize: 1, MaxPacketBytes: 65535}},
		device:     &recordingPacketDevice{writes: make(chan []byte, 1)},
		ports:      make(map[turntf.UserRef]*peerPort),
		relayPorts: make(map[turntf.UserRef]map[string]*peerPort),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newFakeRelayConn("relay-sync-close")
	conn.synchronousCloseCallbacks = true
	port := r.registerPort(ctx, peer, conn)

	closed := make(chan struct{})
	go func() {
		port.close()
		close(closed)
	}()
	waitForDone(t, closed, "port close deadlocked in synchronous OnClose callback")
	waitForDone(t, port.done, "port done was not closed")

	r.mu.RLock()
	selected := r.ports[peerRef]
	remaining := len(r.relayPorts[peerRef])
	r.mu.RUnlock()
	if selected != nil || remaining != 0 {
		t.Fatalf("closed port remained registered: selected=%p remaining=%d", selected, remaining)
	}
}

func TestRegisterPortKeepsWarmRelayWhileStreamIsActive(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 2, UserID: 1}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	device := &recordingPacketDevice{writes: make(chan []byte, 1)}
	r := &Runtime{
		cfg:           Config{Transport: TransportConfig{SendQueueSize: 8, MaxPacketBytes: 65535}},
		device:        device,
		ports:         make(map[turntf.UserRef]*peerPort),
		relayPorts:    make(map[turntf.UserRef]map[string]*peerPort),
		activeStreams: map[turntf.UserRef]*streamPort{peerRef: testStreamPort()},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newFakeRelayConn("warm-relay")
	port := r.registerPort(ctx, peer, conn)
	r.mu.RLock()
	selected := r.ports[peerRef]
	live := len(r.relayPorts[peerRef])
	r.mu.RUnlock()
	if selected != port || live != 1 {
		t.Fatalf("warm fallback selected=%p live=%d, want registered port", selected, live)
	}
	select {
	case <-conn.closed:
		t.Fatal("warm Relay was closed while stream was active")
	default:
	}
	packet := ipv4Packet(10, 0, 0, 2)
	batch, ok := appendTunBatch(nil, packet)
	if !ok {
		t.Fatal("append warm Relay packet")
	}
	conn.receive <- batch
	select {
	case got := <-device.writes:
		if string(got) != string(packet) {
			t.Fatalf("warm Relay receive = %v, want %v", got, packet)
		}
	case <-time.After(time.Second):
		t.Fatal("warm Relay did not remain receive-capable")
	}
	port.close()
}

func TestRegisterPortKeepsFiveSessionReceivePathsAndPromotesOutbound(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 2, UserID: 1}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	device := &recordingPacketDevice{writes: make(chan []byte, 8)}
	r := &Runtime{
		cfg:           Config{Transport: TransportConfig{SendQueueSize: 8, MaxPacketBytes: 65535}},
		device:        device,
		ports:         make(map[turntf.UserRef]*peerPort),
		relayPorts:    make(map[turntf.UserRef]map[string]*peerPort),
		activeStreams: make(map[turntf.UserRef]*streamPort),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ids := []string{"relay-z", "relay-y", "relay-a", "relay-m", "relay-b"}
	connections := make(map[string]*fakeRelayConn, len(ids))
	ports := make(map[string]*peerPort, len(ids))
	for _, id := range ids {
		conn := newFakeRelayConn(id)
		connections[id] = conn
		ports[id] = r.registerPort(ctx, peer, conn)
	}

	r.mu.RLock()
	selected := r.ports[peerRef]
	live := len(r.relayPorts[peerRef])
	r.mu.RUnlock()
	if selected != ports["relay-a"] || live != 5 {
		t.Fatalf("selected=%p live=%d, want relay-a and 5", selected, live)
	}
	for _, id := range ids {
		select {
		case <-connections[id].closed:
			t.Fatalf("receive path %s was aborted during selection", id)
		default:
		}
		packet := ipv4Packet(10, 0, 0, byte(len(id)))
		batch, ok := appendTunBatch(nil, packet)
		if !ok {
			t.Fatalf("append packet for %s", id)
		}
		connections[id].receive <- batch
		select {
		case got := <-device.writes:
			if string(got) != string(packet) {
				t.Fatalf("received packet for %s = %v, want %v", id, got, packet)
			}
		case <-time.After(time.Second):
			t.Fatalf("receive path %s did not reach TUN", id)
		}
	}

	outbound := ipv4Packet(10, 1, 0, 1)
	if !selected.enqueue(outbound) {
		t.Fatal("selected outbound queue rejected packet")
	}
	select {
	case <-connections["relay-a"].sent:
	case <-time.After(time.Second):
		t.Fatal("lowest relay ID did not carry outbound packet")
	}

	connections["relay-a"].Abort(errors.New("reconnect"))
	waitForCondition(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.ports[peerRef] == ports["relay-b"] && len(r.relayPorts[peerRef]) == 4
	}, "closed selected connection was not removed and promoted")
	connections["relay-z"].Abort(errors.New("late close"))
	waitForCondition(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.ports[peerRef] == ports["relay-b"] && len(r.relayPorts[peerRef]) == 3
	}, "late non-owner close changed the selected connection")

	r.closePorts()
}

func TestRegisterPortConcurrentReconnectStress(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 9, UserID: 9}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	r := &Runtime{
		cfg:           Config{Transport: TransportConfig{SendQueueSize: 4, MaxPacketBytes: 65535}},
		device:        &recordingPacketDevice{writes: make(chan []byte, 1)},
		ports:         make(map[turntf.UserRef]*peerPort),
		relayPorts:    make(map[turntf.UserRef]map[string]*peerPort),
		activeStreams: make(map[turntf.UserRef]*streamPort),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for round := 0; round < 40; round++ {
		connections := make([]*fakeRelayConn, 5)
		ports := make([]*peerPort, 5)
		var registered sync.WaitGroup
		for i := range connections {
			connections[i] = newFakeRelayConn(fmt.Sprintf("round-%02d-relay-%d", round, i))
			registered.Add(1)
			go func(i int) {
				defer registered.Done()
				ports[i] = r.registerPort(ctx, peer, connections[i])
			}(i)
		}
		registered.Wait()
		waitForCondition(t, func() bool {
			r.mu.RLock()
			defer r.mu.RUnlock()
			return len(r.relayPorts[peerRef]) == 5 && r.ports[peerRef] == ports[0]
		}, "concurrent registration did not converge")

		var staleCloses sync.WaitGroup
		for i := 4; i >= 1; i-- {
			staleCloses.Add(1)
			go func(i int) {
				defer staleCloses.Done()
				connections[i].Abort(errors.New("stale reconnect close"))
			}(i)
		}
		staleCloses.Wait()
		waitForCondition(t, func() bool {
			r.mu.RLock()
			defer r.mu.RUnlock()
			return len(r.relayPorts[peerRef]) == 1 && r.ports[peerRef] == ports[0]
		}, "stale closes removed the active owner")
		connections[0].Abort(errors.New("selected reconnect close"))
		waitForCondition(t, func() bool {
			r.mu.RLock()
			defer r.mu.RUnlock()
			return len(r.relayPorts[peerRef]) == 0 && r.ports[peerRef] == nil
		}, "selected close left stale ownership")
	}
}

func waitForNamedDial(t *testing.T, dials <-chan string, want string) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case name := <-dials:
			if name == want {
				return
			}
		case <-timer.C:
			t.Fatalf("peer %q did not start Relay dial loop", want)
		}
	}
}

func waitForGroup(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	waitForDone(t, done, "peer loops did not stop")
}

type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *recordingLogger) last() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.lines) == 0 {
		return ""
	}
	return l.lines[len(l.lines)-1]
}

type queuedPacketDevice struct {
	mu      sync.Mutex
	packets [][]byte
}

func (d *queuedPacketDevice) Name() string { return "test" }
func (d *queuedPacketDevice) ReadPacket(ctx context.Context) ([]byte, error) {
	d.mu.Lock()
	if len(d.packets) > 0 {
		packet := d.packets[0]
		d.packets = d.packets[1:]
		d.mu.Unlock()
		return packet, nil
	}
	d.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}
func (d *queuedPacketDevice) WritePacket([]byte) error { return nil }
func (d *queuedPacketDevice) Close() error             { return nil }

type recordingPacketDevice struct {
	writes chan []byte
}

func (d *recordingPacketDevice) Name() string { return "test" }
func (d *recordingPacketDevice) ReadPacket(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (d *recordingPacketDevice) WritePacket(packet []byte) error {
	d.writes <- append([]byte(nil), packet...)
	return nil
}
func (d *recordingPacketDevice) Close() error { return nil }

type fakeRelayConn struct {
	id                        string
	receive                   chan []byte
	sent                      chan []byte
	closed                    chan struct{}
	once                      sync.Once
	mu                        sync.Mutex
	callbacks                 []func(error)
	synchronousCloseCallbacks bool
}

func newFakeRelayConn(id string) *fakeRelayConn {
	return &fakeRelayConn{id: id, receive: make(chan []byte, 1), sent: make(chan []byte, 16), closed: make(chan struct{})}
}

func (c *fakeRelayConn) RelayID() string { return c.id }
func (c *fakeRelayConn) Send(payload []byte) error {
	select {
	case c.sent <- append([]byte(nil), payload...):
		return nil
	case <-c.closed:
		return &turntf.RelayError{Code: turntf.RelayErrorClientClosed, Message: "closed"}
	}
}
func (c *fakeRelayConn) ReceiveTimeout(timeout time.Duration) ([]byte, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case packet := <-c.receive:
		return packet, nil
	case <-c.closed:
		return nil, &turntf.RelayError{Code: turntf.RelayErrorClientClosed, Message: "closed"}
	case <-timer.C:
		return nil, &turntf.RelayError{Code: turntf.RelayErrorReceiveTimeout, Message: "timeout"}
	}
}
func (c *fakeRelayConn) Abort(err error) {
	c.once.Do(func() {
		close(c.closed)
		c.mu.Lock()
		callbacks := append([]func(error){}, c.callbacks...)
		c.mu.Unlock()
		for _, callback := range callbacks {
			if c.synchronousCloseCallbacks {
				callback(err)
			} else {
				go callback(err)
			}
		}
	})
}
func (c *fakeRelayConn) OnClose(callback func(error)) {
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		callback(errors.New("closed"))
	default:
		c.callbacks = append(c.callbacks, callback)
		c.mu.Unlock()
	}
}

func ipv4Packet(a, b, c, d byte) []byte {
	packet := make([]byte, 20)
	packet[0] = 0x45
	packet[16], packet[17], packet[18], packet[19] = a, b, c, d
	return packet
}
