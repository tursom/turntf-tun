package tun

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	turntf "github.com/tursom/turntf-go"
)

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

func ipv4Packet(a, b, c, d byte) []byte {
	packet := make([]byte, 20)
	packet[0] = 0x45
	packet[16], packet[17], packet[18], packet[19] = a, b, c, d
	return packet
}
