package tun

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	turntf "github.com/tursom/turntf-go"
)

func testStreamPort() *streamPort {
	id := turntf.StreamID{1, 2, 3}
	ctx, cancel := context.WithCancel(context.Background())
	return &streamPort{
		id: id, sender: turntf.NewStreamSenderState(id, 1, 1024),
		queue: make(chan []byte, 4), ready: make(chan struct{}), lost: make(chan struct{}, 1),
		epoch: 1, attemptErr: make(chan error, 1), ctx: ctx, cancel: cancel,
		done: make(chan struct{}), restart: make(chan struct{}),
	}
}

func TestStreamBatchRoundTrip(t *testing.T) {
	queue := make(chan []byte, 2)
	queue <- []byte("second")
	queue <- []byte("third")
	payload, overflow := encodeStreamBatch([]byte("first"), queue)
	if overflow != nil {
		t.Fatalf("unexpected overflow packet: %q", overflow)
	}
	packets, ok := decodeStreamBatch(payload)
	if !ok || len(packets) != 3 {
		t.Fatalf("decoded batch = (%v, %v), want three packets", packets, ok)
	}
	for i, want := range []string{"first", "second", "third"} {
		if string(packets[i]) != want {
			t.Fatalf("packet %d = %q, want %q", i, packets[i], want)
		}
	}
}

func TestStreamBatchPreservesOverflowPacket(t *testing.T) {
	first := make([]byte, streamBatchMax-8)
	queue := make(chan []byte, 1)
	queue <- []byte("overflow")
	payload, overflow := encodeStreamBatch(first, queue)
	if len(payload) != len(first)+4 {
		t.Fatalf("batch length = %d, want %d", len(payload), len(first)+4)
	}
	if string(overflow) != "overflow" {
		t.Fatalf("overflow = %q, want preserved packet", overflow)
	}
}

func TestStreamBatchRejectsMalformedPayload(t *testing.T) {
	if packets, ok := decodeStreamBatch([]byte{streamPacketMagic[0], streamPacketMagic[1], 0, 4, 'x'}); ok || packets != nil {
		t.Fatalf("malformed batch decoded as (%v, %v)", packets, ok)
	}
}
func TestStreamLoopRetriesAfterFallbackAndActivatesStream(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 2, UserID: 3}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "session"}
	var openCount atomic.Int32
	var idCount atomic.Uint32
	var dialCount atomic.Int32
	fallbackStarted := make(chan *peerPort, 1)
	r := &Runtime{
		cfg:           Config{Transport: TransportConfig{SendQueueSize: 4, DialRetryInterval: Duration{Duration: 10 * time.Millisecond}}},
		ports:         make(map[turntf.UserRef]*peerPort),
		activeStreams: make(map[turntf.UserRef]*streamPort),
		streamPorts:   make(map[turntf.UserRef]*streamPort),
		streamRecv:    make(map[turntf.StreamID]*streamReceiver),
		localUser:     turntf.UserRef{NodeID: 1, UserID: 1},
		streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{{Session: session, TransientCapable: true}}}, nil
		},
		streamNewID: func() (turntf.StreamID, error) {
			return turntf.StreamID{byte(idCount.Add(1))}, nil
		},
	}
	r.streamSend = func(ctx context.Context, _ turntf.UserRef, _ turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
		if frame.Kind != turntf.StreamFrameOpen {
			return turntf.RelayAccepted{}, nil
		}
		if openCount.Add(1) == 1 {
			return turntf.RelayAccepted{}, errors.New("initial open failed")
		}
		runtimeHandler{runtime: r}.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: session}, turntf.StreamFrame{Kind: turntf.StreamFrameOpenAck, ID: frame.ID, Epoch: frame.Epoch, Window: frame.Window})
		return turntf.RelayAccepted{}, nil
	}
	r.relayDial = func(ctx context.Context, _ PeerConfig) {
		dialCount.Add(1)
		port := &peerPort{queue: make(chan []byte, 1), done: make(chan struct{})}
		r.mu.Lock()
		if r.activeStreams[peerRef] == nil {
			r.ports[peerRef] = port
		}
		r.mu.Unlock()
		fallbackStarted <- port
		select {
		case <-ctx.Done():
			port.close()
		case <-port.done:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		r.streamLoop(ctx, peer)
		close(loopDone)
	}()
	fallbackPort := <-fallbackStarted
	waitForCondition(t, func() bool {
		r.mu.RLock()
		active := r.activeStreams[peerRef]
		relay := r.ports[peerRef]
		r.mu.RUnlock()
		r.streamMu.RLock()
		registered := r.streamPorts[peerRef]
		r.streamMu.RUnlock()
		return openCount.Load() >= 2 && active != nil && active == registered && relay == fallbackPort
	}, "stream did not activate while retaining warm Relay fallback")
	select {
	case <-fallbackPort.done:
		t.Fatal("warm Relay fallback closed after one stream direction activated")
	default:
	}
	if got := dialCount.Load(); got != 1 {
		t.Fatalf("fallback dial count = %d, want 1", got)
	}
	cancel()
	waitForDone(t, loopDone, "stream lifecycle did not stop")
}

func TestStreamLoopNonDialSideOnlyRetriesStream(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 2, UserID: 3}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	var openCount atomic.Int32
	var dialCount atomic.Int32
	r := failingStreamRuntime(peerRef, turntf.UserRef{NodeID: 9, UserID: 1}, &openCount)
	r.relayDial = func(context.Context, PeerConfig) { dialCount.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		r.streamLoop(ctx, peer)
		close(loopDone)
	}()
	waitForCondition(t, func() bool { return openCount.Load() >= 3 }, "non-dial side did not retry stream")
	cancel()
	waitForDone(t, loopDone, "non-dial stream lifecycle did not stop")
	if got := dialCount.Load(); got != 0 {
		t.Fatalf("fallback dial count = %d, want 0", got)
	}
}

func TestStreamLoopRepeatedFailuresKeepSingleFallbackDial(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 2, UserID: 3}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	var openCount atomic.Int32
	var dialCount atomic.Int32
	var activeDials atomic.Int32
	var maxActiveDials atomic.Int32
	r := failingStreamRuntime(peerRef, turntf.UserRef{NodeID: 1, UserID: 1}, &openCount)
	r.relayDial = func(ctx context.Context, _ PeerConfig) {
		dialCount.Add(1)
		active := activeDials.Add(1)
		for {
			maximum := maxActiveDials.Load()
			if active <= maximum || maxActiveDials.CompareAndSwap(maximum, active) {
				break
			}
		}
		<-ctx.Done()
		activeDials.Add(-1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		r.streamLoop(ctx, peer)
		close(loopDone)
	}()
	waitForCondition(t, func() bool { return openCount.Load() >= 4 }, "stream failures were not retried")
	if got := dialCount.Load(); got != 1 {
		t.Fatalf("fallback dial count = %d, want 1", got)
	}
	if got := maxActiveDials.Load(); got != 1 {
		t.Fatalf("maximum concurrent fallback dials = %d, want 1", got)
	}
	cancel()
	waitForDone(t, loopDone, "failing stream lifecycle did not stop")
	if got := activeDials.Load(); got != 0 {
		t.Fatalf("active fallback dials after cancellation = %d, want 0", got)
	}
}

func failingStreamRuntime(peer, local turntf.UserRef, openCount *atomic.Int32) *Runtime {
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "session"}
	var idCount atomic.Uint32
	return &Runtime{
		cfg:           Config{Transport: TransportConfig{SendQueueSize: 4, DialRetryInterval: Duration{Duration: time.Millisecond}}},
		ports:         make(map[turntf.UserRef]*peerPort),
		activeStreams: make(map[turntf.UserRef]*streamPort),
		streamPorts:   make(map[turntf.UserRef]*streamPort),
		streamRecv:    make(map[turntf.StreamID]*streamReceiver),
		localUser:     local,
		streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{{Session: session, TransientCapable: true}}}, nil
		},
		streamNewID: func() (turntf.StreamID, error) {
			return turntf.StreamID{byte(idCount.Add(1))}, nil
		},
		streamSend: func(_ context.Context, _ turntf.UserRef, _ turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			if frame.Kind == turntf.StreamFrameOpen {
				openCount.Add(1)
			}
			return turntf.RelayAccepted{}, errors.New("open failed")
		},
	}
}

func TestRunStreamTriesRemainingSessionsAndIgnoresLateOpenAck(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 2, UserID: 3}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	staleSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "stale"}
	currentSession := turntf.SessionRef{ServingNodeID: 5, SessionID: "current"}
	secondOpen := make(chan sentStreamFrame, 1)
	ready := make(chan struct{}, 1)
	var mu sync.Mutex
	var sent []sentStreamFrame
	r := &Runtime{
		cfg:           Config{Transport: TransportConfig{SendQueueSize: 4, DialRetryInterval: Duration{Duration: time.Second}}},
		activeStreams: make(map[turntf.UserRef]*streamPort),
		streamPorts:   make(map[turntf.UserRef]*streamPort),
		streamRecv:    make(map[turntf.StreamID]*streamReceiver),
		streamTimeout: 5 * time.Millisecond,
		streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{
				{Session: staleSession, TransientCapable: true},
				{Session: currentSession, TransientCapable: true},
			}}, nil
		},
		streamNewID: func() (turntf.StreamID, error) { return turntf.StreamID{7}, nil },
		streamSend: func(_ context.Context, _ turntf.UserRef, target turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			if frame.Kind == turntf.StreamFrameOpen {
				mu.Lock()
				sent = append(sent, sentStreamFrame{session: target, frame: frame})
				mu.Unlock()
				if target == currentSession {
					secondOpen <- sentStreamFrame{session: target, frame: frame}
				}
			}
			return turntf.RelayAccepted{}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan streamRunResult, 1)
	go func() {
		result <- r.runStream(ctx, peer, func() { ready <- struct{}{} }, func() {})
	}()

	open := waitForStreamFrame(t, secondOpen, turntf.StreamFrameOpen)
	handler := runtimeHandler{runtime: r}
	handler.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: staleSession}, turntf.StreamFrame{Kind: turntf.StreamFrameOpenAck, ID: open.frame.ID, Epoch: open.frame.Epoch, Window: open.frame.Window})
	select {
	case <-ready:
		t.Fatal("late stale-session OpenAck activated the current candidate")
	default:
	}
	handler.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: currentSession}, turntf.StreamFrame{Kind: turntf.StreamFrameOpenAck, ID: open.frame.ID, Epoch: open.frame.Epoch, Window: open.frame.Window})
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("current-session OpenAck did not activate stream")
	}
	cancel()
	select {
	case got := <-result:
		if got != streamRunStopped {
			t.Fatalf("runStream result = %v, want stopped", got)
		}
	case <-time.After(time.Second):
		t.Fatal("runStream did not stop")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 2 || sent[0].session != staleSession || sent[1].session != currentSession {
		t.Fatalf("Open attempts = %+v, want stale then current", sent)
	}
}

func TestRecoverStreamTriesRemainingSessionsAndIgnoresLateAck(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 20, UserID: 30}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	staleSession := turntf.SessionRef{ServingNodeID: 40, SessionID: "stale"}
	currentSession := turntf.SessionRef{ServingNodeID: 41, SessionID: "current"}
	port := testStreamPort()
	port.peer = peerRef
	port.targetSession = staleSession
	port.readyState = true
	close(port.ready)
	if _, err := port.sender.Data([]byte("pending")); err != nil {
		t.Fatal(err)
	}
	secondResume := make(chan sentStreamFrame, 1)
	var mu sync.Mutex
	var sent []sentStreamFrame
	r := &Runtime{
		cfg:           Config{Transport: TransportConfig{DialRetryInterval: Duration{Duration: time.Second}}},
		streamPorts:   map[turntf.UserRef]*streamPort{peerRef: port},
		streamTimeout: 5 * time.Millisecond,
		streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{
				{Session: staleSession, TransientCapable: true},
				{Session: currentSession, TransientCapable: true},
			}}, nil
		},
		streamSend: func(_ context.Context, _ turntf.UserRef, target turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			mu.Lock()
			sent = append(sent, sentStreamFrame{session: target, frame: frame})
			mu.Unlock()
			if frame.Kind == turntf.StreamFrameResume && target == currentSession {
				secondResume <- sentStreamFrame{session: target, frame: frame}
			}
			return turntf.RelayAccepted{}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recovered := make(chan bool, 1)
	go func() { recovered <- r.recoverStream(ctx, peer, port) }()
	resume := waitForStreamFrame(t, secondResume, turntf.StreamFrameResume)
	handler := runtimeHandler{runtime: r}
	ack := turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: resume.frame.Epoch, Offset: resume.frame.Offset, Window: turntf.DefaultStreamWindow}
	handler.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: staleSession}, ack)
	select {
	case <-recovered:
		t.Fatal("late stale-session ACK completed current Resume")
	default:
	}
	handler.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: currentSession}, ack)
	select {
	case ok := <-recovered:
		if !ok {
			t.Fatal("recovery failed")
		}
	case <-time.After(time.Second):
		t.Fatal("current-session ACK did not complete recovery")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 3 || sent[0].frame.Kind != turntf.StreamFrameResume || sent[0].session != staleSession || sent[1].frame.Kind != turntf.StreamFrameResume || sent[1].session != currentSession || sent[2].frame.Kind != turntf.StreamFrameData || sent[2].session != currentSession {
		t.Fatalf("recovery attempts = %+v, want stale Resume, current Resume, current Data", sent)
	}
}

func waitForCondition(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(message)
}

func waitForDone(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func TestStreamPortResumeKeepsIDAndPendingSuffix(t *testing.T) {
	p := testStreamPort()
	if !p.enqueue([]byte("queued")) {
		t.Fatal("packet was not queued")
	}
	frame, err := p.sender.Data([]byte("unacknowledged"))
	if err != nil {
		t.Fatal(err)
	}
	if frame.ID != p.id {
		t.Fatalf("sender ID = %v, want %v", frame.ID, p.id)
	}
	session := turntf.SessionRef{ServingNodeID: 2, SessionID: "session-3"}
	lastAck, ready, _ := p.beginResume(session, 2)
	frames, err := p.sender.Resume(2, lastAck)
	if err != nil {
		t.Fatal(err)
	}
	if lastAck != 0 || len(frames) != 1 || string(frames[0].Payload) != "unacknowledged" || frames[0].Epoch != 2 {
		t.Fatalf("unexpected resume: offset=%d frames=%+v", lastAck, frames)
	}
	select {
	case <-ready:
		t.Fatal("resume became ready before ACK")
	default:
	}
	p.acknowledge(session, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: p.id, Epoch: 2, Offset: frame.Offset + uint64(len(frame.Payload))})
	select {
	case <-ready:
	default:
		t.Fatal("resume ACK did not release replay")
	}
	if p.readyState {
		t.Fatal("resume ACK opened path before pending replay completed")
	}
	if !p.completeResume(session, 2) {
		t.Fatal("pending replay did not complete Resume")
	}
	select {
	case <-p.ready:
	default:
		t.Fatal("completed Resume did not release path")
	}
	if got := <-p.queue; string(got) != "queued" {
		t.Fatalf("queued packet = %q", got)
	}
}

func TestStreamPortRejectsInvalidResumeAck(t *testing.T) {
	p := testStreamPort()
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "session"}
	frame, err := p.sender.Data([]byte("pending"))
	if err != nil {
		t.Fatal(err)
	}
	_, ready, _ := p.beginResume(session, 2)
	if _, err := p.sender.Resume(2, 0); err != nil {
		t.Fatal(err)
	}
	p.acknowledge(session, turntf.StreamFrame{
		Kind:   turntf.StreamFrameAck,
		ID:     p.id,
		Epoch:  2,
		Offset: frame.Offset + uint64(len(frame.Payload)) + 1,
		Window: turntf.DefaultStreamWindow,
	})
	select {
	case <-ready:
		t.Fatal("invalid Resume ACK activated path")
	default:
	}
	if p.readyState || p.lastAck != 0 {
		t.Fatalf("invalid Resume ACK changed state: ready=%v last_ack=%d", p.readyState, p.lastAck)
	}
}

func TestRuntimeDisconnectInvalidatesReadyStreamsOnce(t *testing.T) {
	firstPeer := turntf.UserRef{NodeID: 2, UserID: 3}
	secondPeer := turntf.UserRef{NodeID: 4, UserID: 5}
	first := testStreamPort()
	second := testStreamPort()
	for _, port := range []*streamPort{first, second} {
		port.readyState = true
		close(port.ready)
	}
	r := &Runtime{
		streamPorts:   map[turntf.UserRef]*streamPort{firstPeer: first, secondPeer: second},
		activeStreams: map[turntf.UserRef]*streamPort{firstPeer: first, secondPeer: second},
	}
	h := runtimeHandler{runtime: r}
	h.OnDisconnect(context.Background(), errors.New("websocket disconnected"))
	for peer, port := range map[turntf.UserRef]*streamPort{firstPeer: first, secondPeer: second} {
		select {
		case <-port.lost:
		default:
			t.Fatalf("peer %v path loss was not signaled", peer)
		}
		if port.readyState {
			t.Fatalf("peer %v remained ready", peer)
		}
		if r.activeStreams[peer] != nil {
			t.Fatalf("peer %v remained active after disconnect", peer)
		}
	}
	h.OnDisconnect(context.Background(), errors.New("duplicate websocket disconnect"))
	for peer, port := range map[turntf.UserRef]*streamPort{firstPeer: first, secondPeer: second} {
		select {
		case <-port.lost:
			t.Fatalf("peer %v received duplicate path-loss signal", peer)
		default:
		}
	}
}

func TestTrackedStreamErrorInvalidatesOnlyMatchingPeerSession(t *testing.T) {
	failedPeer := turntf.UserRef{NodeID: 2, UserID: 3}
	healthyPeer := turntf.UserRef{NodeID: 4, UserID: 5}
	failedSession := turntf.SessionRef{ServingNodeID: 6, SessionID: "failed"}
	healthySession := turntf.SessionRef{ServingNodeID: 7, SessionID: "healthy"}
	failed := testStreamPort()
	failed.peer = failedPeer
	failed.targetSession = failedSession
	healthy := testStreamPort()
	healthy.peer = healthyPeer
	healthy.targetSession = healthySession
	for _, port := range []*streamPort{failed, healthy} {
		port.readyState = true
		close(port.ready)
	}
	failedRelay := newFakeRelayConn("failed-relay")
	healthyRelay := newFakeRelayConn("healthy-relay")
	failedRelayPort := &peerPort{conn: failedRelay, done: make(chan struct{})}
	healthyRelayPort := &peerPort{conn: healthyRelay, done: make(chan struct{})}
	r := &Runtime{
		streamPorts:   map[turntf.UserRef]*streamPort{failedPeer: failed, healthyPeer: healthy},
		activeStreams: map[turntf.UserRef]*streamPort{failedPeer: failed, healthyPeer: healthy},
		ports:         map[turntf.UserRef]*peerPort{failedPeer: failedRelayPort, healthyPeer: healthyRelayPort},
		relayPorts: map[turntf.UserRef]map[string]*peerPort{
			failedPeer:  {failedRelay.RelayID(): failedRelayPort},
			healthyPeer: {healthyRelay.RelayID(): healthyRelayPort},
		},
		preferredStreams: map[turntf.UserRef]turntf.SessionRef{
			failedPeer:  failedSession,
			healthyPeer: healthySession,
		},
	}
	h := runtimeHandler{runtime: r}
	h.OnStreamSendResult(context.Background(), turntf.StreamSendResult{
		RequestID: 1,
		Metadata:  turntf.StreamSendMetadata{Target: failedPeer, TargetSession: failedSession, StreamID: failed.id, Kind: turntf.StreamFrameData, Epoch: failed.epoch},
		Err:       errors.New("stream target session unavailable"),
	})
	select {
	case <-failed.lost:
	default:
		t.Fatal("matching peer path loss was not signaled")
	}
	if failed.readyState {
		t.Fatal("matching peer remained ready")
	}
	select {
	case <-healthy.lost:
		t.Fatal("healthy peer was invalidated by another peer's send error")
	default:
	}
	if !healthy.readyState {
		t.Fatal("healthy peer was marked not ready")
	}
	select {
	case <-failedRelay.closed:
		t.Fatal("stream failure closed the failed peer warm Relay")
	default:
	}
	select {
	case <-healthyRelay.closed:
		t.Fatal("healthy peer Relay was closed")
	default:
	}
	if r.activeStreams[failedPeer] != nil {
		t.Fatal("failed stream remained active")
	}
	if r.activeStreams[healthyPeer] != healthy {
		t.Fatal("healthy stream was removed from active paths")
	}
	r.streamMu.RLock()
	_, failedPreferred := r.preferredStreams[failedPeer]
	gotHealthy := r.preferredStreams[healthyPeer]
	r.streamMu.RUnlock()
	if failedPreferred || gotHealthy != healthySession {
		t.Fatalf("preferred sessions after failure: failed_present=%v healthy=%+v", failedPreferred, gotHealthy)
	}
}

func TestTrackedStreamErrorForOldStreamIDDoesNotInvalidateCurrentPath(t *testing.T) {
	peer := turntf.UserRef{NodeID: 2, UserID: 3}
	session := turntf.SessionRef{ServingNodeID: 6, SessionID: "current"}
	port := testStreamPort()
	port.peer = peer
	port.id = turntf.StreamID{2}
	port.targetSession = session
	port.readyState = true
	close(port.ready)
	relay := newFakeRelayConn("warm-relay")
	relayPort := &peerPort{conn: relay, done: make(chan struct{})}
	r := &Runtime{
		streamPorts:      map[turntf.UserRef]*streamPort{peer: port},
		activeStreams:    map[turntf.UserRef]*streamPort{peer: port},
		ports:            map[turntf.UserRef]*peerPort{peer: relayPort},
		relayPorts:       map[turntf.UserRef]map[string]*peerPort{peer: {relay.RelayID(): relayPort}},
		preferredStreams: map[turntf.UserRef]turntf.SessionRef{peer: session},
	}
	runtimeHandler{runtime: r}.OnStreamSendResult(context.Background(), turntf.StreamSendResult{
		RequestID: 3,
		Metadata:  turntf.StreamSendMetadata{Target: peer, TargetSession: session, StreamID: turntf.StreamID{1}, Kind: turntf.StreamFrameData, Epoch: port.epoch},
		Err:       errors.New("late error from old stream"),
	})
	select {
	case <-port.lost:
		t.Fatal("late old-stream error invalidated current path")
	default:
	}
	select {
	case <-relay.closed:
		t.Fatal("late old-stream error closed warm Relay")
	default:
	}
	if !port.readyState || r.activeStreams[peer] != port || r.preferredStreams[peer] != session {
		t.Fatal("late old-stream error changed current path state")
	}
}

func TestTrackedStreamErrorForOldSessionDoesNotInvalidateNewPath(t *testing.T) {
	peer := turntf.UserRef{NodeID: 2, UserID: 3}
	oldSession := turntf.SessionRef{ServingNodeID: 6, SessionID: "old"}
	newSession := turntf.SessionRef{ServingNodeID: 6, SessionID: "new"}
	port := testStreamPort()
	port.peer = peer
	port.targetSession = newSession
	port.readyState = true
	close(port.ready)
	r := &Runtime{streamPorts: map[turntf.UserRef]*streamPort{peer: port}, preferredStreams: map[turntf.UserRef]turntf.SessionRef{peer: newSession}}
	runtimeHandler{runtime: r}.OnStreamSendResult(context.Background(), turntf.StreamSendResult{
		RequestID: 2,
		Metadata:  turntf.StreamSendMetadata{Target: peer, TargetSession: oldSession, StreamID: port.id, Kind: turntf.StreamFrameData, Epoch: port.epoch},
		Err:       errors.New("late error from old session"),
	})
	select {
	case <-port.lost:
		t.Fatal("late old-session error invalidated current path")
	default:
	}
	if !port.readyState {
		t.Fatal("current path was marked not ready")
	}
}

func TestTrackedStreamErrorForOldEpochDoesNotInvalidateResumedPath(t *testing.T) {
	peer := turntf.UserRef{NodeID: 2, UserID: 3}
	session := turntf.SessionRef{ServingNodeID: 6, SessionID: "session"}
	port := testStreamPort()
	port.peer = peer
	port.targetSession = session
	port.epoch = 2
	port.readyState = true
	close(port.ready)
	r := &Runtime{
		streamPorts:      map[turntf.UserRef]*streamPort{peer: port},
		activeStreams:    map[turntf.UserRef]*streamPort{peer: port},
		preferredStreams: map[turntf.UserRef]turntf.SessionRef{peer: session},
	}
	runtimeHandler{runtime: r}.OnStreamSendResult(context.Background(), turntf.StreamSendResult{
		RequestID: 4,
		Metadata:  turntf.StreamSendMetadata{Target: peer, TargetSession: session, StreamID: port.id, Kind: turntf.StreamFrameData, Epoch: 1},
		Err:       errors.New("late error from old epoch"),
	})
	select {
	case <-port.lost:
		t.Fatal("late old-epoch error invalidated resumed path")
	default:
	}
	if !port.readyState || r.activeStreams[peer] != port || r.preferredStreams[peer] != session {
		t.Fatal("late old-epoch error changed resumed path state")
	}
}

func TestWriteStreamLoopUsesRecoveredSessionAfterWaitingForPacket(t *testing.T) {
	peer := turntf.UserRef{NodeID: 2, UserID: 3}
	oldSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "old"}
	newSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "new"}
	port := testStreamPort()
	port.peer = peer
	port.targetSession = oldSession
	port.readyState = true
	close(port.ready)
	sent := make(chan sentStreamFrame, 1)
	r := &Runtime{streamSend: func(_ context.Context, _ turntf.UserRef, session turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
		sent <- sentStreamFrame{session: session, frame: frame}
		return turntf.RelayAccepted{}, nil
	}}
	done := make(chan struct{})
	go func() {
		r.writeStreamLoop(port)
		close(done)
	}()

	// Let the writer reach its empty-queue wait on the original path.
	time.Sleep(20 * time.Millisecond)
	port.markPathLost()
	_, _, _ = port.beginResume(newSession, 2)
	if _, err := port.sender.Resume(2, 0); err != nil {
		t.Fatal(err)
	}
	port.acknowledge(newSession, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: 2, Offset: 0, Window: turntf.DefaultStreamWindow})
	if !port.completeResume(newSession, 2) {
		t.Fatal("complete Resume")
	}
	port.queue <- []byte("after-recovery")

	select {
	case got := <-sent:
		packets, batched := decodeStreamBatch(got.frame.Payload)
		if got.session != newSession || got.frame.Epoch != 2 || !batched || len(packets) != 1 || string(packets[0]) != "after-recovery" {
			t.Fatalf("post-recovery send = %+v packets=%q, want new session at epoch 2", got, packets)
		}
	case <-time.After(time.Second):
		t.Fatal("post-recovery packet was not sent")
	}
	port.close()
	waitForDone(t, done, "stream writer did not stop")
}

func TestWriteStreamLoopInvalidatesPathWhenAcknowledgementsStall(t *testing.T) {
	peer := turntf.UserRef{NodeID: 2, UserID: 3}
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "stalled"}
	port := testStreamPort()
	port.peer = peer
	port.targetSession = session
	port.sender = turntf.NewStreamSenderState(port.id, 1, 1)
	port.readyState = true
	close(port.ready)
	r := &Runtime{streamTimeout: 5 * time.Millisecond}
	done := make(chan struct{})
	go func() {
		r.writeStreamLoop(port)
		close(done)
	}()
	port.queue <- []byte("blocked")
	select {
	case <-port.lost:
	case <-time.After(time.Second):
		t.Fatal("stalled acknowledgement did not invalidate path")
	}
	if port.readyState {
		t.Fatal("stalled acknowledgement left path ready")
	}
	port.close()
	waitForDone(t, done, "stream writer did not stop")
}

func TestStreamWindowStallRequiresContinuousLackOfProgress(t *testing.T) {
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "session"}
	other := turntf.SessionRef{ServingNodeID: 4, SessionID: "other"}
	start := time.Unix(100, 0)
	timeout := 5 * time.Second
	var stalled streamWindowStall
	if stalled.observe(session, 1, 0, 10, start, timeout) {
		t.Fatal("first full-window observation reported a stall")
	}
	if stalled.observe(session, 1, 1, 10, start.Add(timeout), timeout) {
		t.Fatal("acknowledgement progress did not reset stall timer")
	}
	if stalled.observe(session, 1, 1, 20, start.Add(2*timeout), timeout) {
		t.Fatal("window credit progress did not reset stall timer")
	}
	if stalled.observe(session, 2, 1, 20, start.Add(3*timeout), timeout) {
		t.Fatal("epoch change did not reset stall timer")
	}
	if stalled.observe(other, 2, 1, 20, start.Add(4*timeout), timeout) {
		t.Fatal("session change did not reset stall timer")
	}
	if stalled.observe(other, 2, 1, 20, start.Add(5*timeout-time.Nanosecond), timeout) {
		t.Fatal("stall reported before a full timeout")
	}
	if !stalled.observe(other, 2, 1, 20, start.Add(5*timeout), timeout) {
		t.Fatal("continuous lack of progress did not report a stall")
	}
}

func TestStreamWindowStallDoesNotInvalidatePathAfterConcurrentAck(t *testing.T) {
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "session"}
	port := testStreamPort()
	port.targetSession = session
	port.readyState = true
	close(port.ready)
	if _, err := port.sender.Data([]byte("x")); err != nil {
		t.Fatal(err)
	}

	// The writer observed ack=0/window=0 as stalled, but a credit update won
	// the race before conditional path invalidation acquired pathMu.
	port.acknowledge(session, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: 1, Offset: 0, Window: 2048})
	if port.failPathIfAckUnchanged(session, 1, 0, 0, errStreamAckStalled) {
		t.Fatal("stale stall observation invalidated path after window progress")
	}
	// Offset progress is protected by the same atomic comparison.
	port.acknowledge(session, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: 1, Offset: 1, Window: 2048})
	if port.failPathIfAckUnchanged(session, 1, 0, 2048, errStreamAckStalled) {
		t.Fatal("stale stall observation invalidated path after offset progress")
	}
	if !port.readyState || port.lastAck != 1 || port.lastWindow != 2048 {
		t.Fatalf("path changed after stale stall observation: ready=%v ack=%d window=%d", port.readyState, port.lastAck, port.lastWindow)
	}
	select {
	case <-port.lost:
		t.Fatal("stale stall observation signaled path loss")
	default:
	}
}

func TestResumeKeepsReadyChannelForWaitingWriter(t *testing.T) {
	peer := turntf.UserRef{NodeID: 2, UserID: 3}
	oldSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "old"}
	newSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "new"}
	port := testStreamPort()
	port.peer = peer
	port.targetSession = oldSession
	port.readyState = true
	close(port.ready)
	port.markPathLost()
	waitingReady := port.ready
	port.queue <- []byte("waiting")
	sent := make(chan sentStreamFrame, 1)
	r := &Runtime{streamSend: func(_ context.Context, _ turntf.UserRef, session turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
		sent <- sentStreamFrame{session: session, frame: frame}
		return turntf.RelayAccepted{}, nil
	}}
	done := make(chan struct{})
	go func() {
		r.writeStreamLoop(port)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	_, _, _ = port.beginResume(newSession, 2)
	if port.ready != waitingReady {
		t.Fatal("beginResume replaced the channel held by a waiting writer")
	}
	if _, err := port.sender.Resume(2, 0); err != nil {
		t.Fatal(err)
	}
	port.acknowledge(newSession, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: 2, Offset: 0, Window: turntf.DefaultStreamWindow})
	if !port.completeResume(newSession, 2) {
		t.Fatal("complete Resume")
	}
	select {
	case got := <-sent:
		if got.session != newSession || got.frame.Epoch != 2 {
			t.Fatalf("waiting writer used stale path: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting writer was not released by completed Resume")
	}
	port.close()
	waitForDone(t, done, "stream writer did not stop")
}

func TestResumeReplayFailureDoesNotOpenWriter(t *testing.T) {
	port := testStreamPort()
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "new"}
	port.readyState = true
	close(port.ready)
	port.markPathLost()
	_, replayReady, _ := port.beginResume(session, 2)
	if _, err := port.sender.Resume(2, 0); err != nil {
		t.Fatal(err)
	}
	port.acknowledge(session, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: 2, Offset: 0, Window: turntf.DefaultStreamWindow})
	select {
	case <-replayReady:
	default:
		t.Fatal("Resume ACK did not release replay")
	}
	if !port.failPath(session, 2, errors.New("replay delivery failed")) {
		t.Fatal("replay failure did not match current path")
	}
	if port.completeResume(session, 2) {
		t.Fatal("failed replay opened writer")
	}
	if port.readyState {
		t.Fatal("failed replay left path ready")
	}
	select {
	case <-port.ready:
		t.Fatal("failed replay closed writer ready channel")
	default:
	}
}

func TestTransportLossDuringResumeDoesNotOpenWriter(t *testing.T) {
	port := testStreamPort()
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "new"}
	port.readyState = true
	close(port.ready)
	port.markPathLost()
	_, replayReady, _ := port.beginResume(session, 2)
	if _, err := port.sender.Resume(2, 0); err != nil {
		t.Fatal(err)
	}
	port.acknowledge(session, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: 2, Offset: 0, Window: turntf.DefaultStreamWindow})
	select {
	case <-replayReady:
	default:
		t.Fatal("Resume ACK did not release replay")
	}
	port.markPathLostWithError(errors.New("transport disconnected"))
	if port.completeResume(session, 2) {
		t.Fatal("transport loss during Resume opened writer")
	}
	select {
	case <-port.ready:
		t.Fatal("transport loss closed writer ready channel")
	default:
	}
}

func TestOldSynchronousSendErrorDoesNotInvalidateRecoveredPath(t *testing.T) {
	peer := turntf.UserRef{NodeID: 2, UserID: 3}
	oldSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "old"}
	newSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "new"}
	port := testStreamPort()
	port.peer = peer
	port.targetSession = oldSession
	port.readyState = true
	close(port.ready)
	sendEntered := make(chan struct{})
	releaseSend := make(chan struct{})
	r := &Runtime{streamSend: func(context.Context, turntf.UserRef, turntf.SessionRef, turntf.StreamFrame, turntf.DeliveryMode) (turntf.RelayAccepted, error) {
		close(sendEntered)
		<-releaseSend
		return turntf.RelayAccepted{}, errors.New("old path send failed")
	}}
	done := make(chan struct{})
	go func() {
		r.writeStreamLoop(port)
		close(done)
	}()
	port.queue <- []byte("old-path")
	waitForDone(t, sendEntered, "old path send did not start")
	port.markPathLost()
	_, _, _ = port.beginResume(newSession, 2)
	if _, err := port.sender.Resume(2, 0); err != nil {
		t.Fatal(err)
	}
	port.acknowledge(newSession, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: 2, Offset: 0, Window: turntf.DefaultStreamWindow})
	if !port.completeResume(newSession, 2) {
		t.Fatal("complete new path Resume")
	}
	close(releaseSend)
	time.Sleep(20 * time.Millisecond)
	if !port.readyState || port.targetSession != newSession || port.epoch != 2 {
		t.Fatalf("old send error invalidated recovered path: ready=%v session=%+v epoch=%d", port.readyState, port.targetSession, port.epoch)
	}
	port.close()
	waitForDone(t, done, "stream writer did not stop")
}

func TestRecoverStreamReplaysPendingBeforeNewData(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 2, UserID: 3}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	oldSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "old"}
	newSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "new"}
	port := testStreamPort()
	port.peer = peerRef
	port.targetSession = oldSession
	port.readyState = true
	close(port.ready)
	oldFrame, err := port.sender.Data([]byte("old-pending"))
	if err != nil {
		t.Fatal(err)
	}
	port.markPathLost()

	replayEntered := make(chan struct{})
	releaseReplay := make(chan struct{})
	newData := make(chan turntf.StreamFrame, 1)
	r := &Runtime{
		cfg: Config{Transport: TransportConfig{DialRetryInterval: Duration{Duration: time.Millisecond}}},
		streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{{Session: newSession, TransientCapable: true}}}, nil
		},
	}
	r.streamSend = func(_ context.Context, _ turntf.UserRef, session turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
		switch frame.Kind {
		case turntf.StreamFrameResume:
			go port.acknowledge(session, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: frame.Epoch, Offset: 0, Window: turntf.DefaultStreamWindow})
		case turntf.StreamFrameData:
			if frame.Offset == oldFrame.Offset {
				close(replayEntered)
				<-releaseReplay
			} else {
				newData <- frame
			}
		}
		return turntf.RelayAccepted{}, nil
	}
	writerDone := make(chan struct{})
	go func() {
		r.writeStreamLoop(port)
		close(writerDone)
	}()
	recovered := make(chan bool, 1)
	go func() { recovered <- r.recoverStream(context.Background(), peer, port) }()
	waitForDone(t, replayEntered, "pending replay did not start")
	port.queue <- []byte("new-data")
	select {
	case frame := <-newData:
		t.Fatalf("new Data overtook pending replay: %+v", frame)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseReplay)
	select {
	case ok := <-recovered:
		if !ok {
			t.Fatal("stream recovery failed")
		}
	case <-time.After(time.Second):
		t.Fatal("stream recovery did not finish")
	}
	select {
	case frame := <-newData:
		wantOffset := oldFrame.Offset + uint64(len(oldFrame.Payload))
		if frame.Offset != wantOffset {
			t.Fatalf("new Data offset = %d, want %d", frame.Offset, wantOffset)
		}
	case <-time.After(time.Second):
		t.Fatal("new Data was not sent after pending replay")
	}
	port.close()
	waitForDone(t, writerDone, "stream writer did not stop")
}

func TestStreamPortPathLossPreservesQueueAndSignalsRecovery(t *testing.T) {
	p := testStreamPort()
	p.readyState = true
	close(p.ready)
	if !p.enqueue([]byte("during-reconnect")) {
		t.Fatal("packet was not queued during reconnect")
	}
	p.markPathLost()
	select {
	case <-p.lost:
	default:
		t.Fatal("path loss was not signaled")
	}
	if p.readyState {
		t.Fatal("path remained ready after loss")
	}
	if got := <-p.queue; string(got) != "during-reconnect" {
		t.Fatalf("queued packet = %q", got)
	}
}

type sentStreamFrame struct {
	session turntf.SessionRef
	frame   turntf.StreamFrame
}

func TestRecoverStreamResolvesCurrentSessionAfterAsyncError(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 20, UserID: 30}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	oldSession := turntf.SessionRef{ServingNodeID: 40, SessionID: "old-session"}
	newSession := turntf.SessionRef{ServingNodeID: 41, SessionID: "new-session"}
	port := testStreamPort()
	port.peer = peerRef
	port.targetSession = oldSession
	port.readyState = true
	close(port.ready)
	pending, err := port.sender.Data([]byte("pending"))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var sent []sentStreamFrame
	r := &Runtime{
		cfg:         Config{Transport: TransportConfig{DialRetryInterval: Duration{Duration: time.Millisecond}}},
		streamPorts: map[turntf.UserRef]*streamPort{peerRef: port},
		streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{{Session: newSession, TransientCapable: true}}}, nil
		},
		streamSend: func(_ context.Context, _ turntf.UserRef, target turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			mu.Lock()
			sent = append(sent, sentStreamFrame{session: target, frame: frame})
			mu.Unlock()
			if frame.Kind == turntf.StreamFrameResume {
				go port.acknowledge(target, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: frame.Epoch, Offset: pending.Offset + uint64(len(pending.Payload)), Window: turntf.DefaultStreamWindow})
			}
			return turntf.RelayAccepted{}, nil
		},
	}
	runtimeHandler{runtime: r}.OnStreamSendResult(context.Background(), turntf.StreamSendResult{
		RequestID: 3,
		Metadata:  turntf.StreamSendMetadata{Target: peerRef, TargetSession: oldSession, StreamID: port.id, Kind: turntf.StreamFrameData, Epoch: port.epoch},
		Err:       errors.New("stream target session unavailable"),
	})
	select {
	case <-port.lost:
	default:
		t.Fatal("asynchronous server error did not signal recovery")
	}
	if !r.recoverStream(context.Background(), peer, port) {
		t.Fatal("stream recovery failed")
	}
	session, ready := port.currentPath()
	if session != newSession || !port.isReady(ready) {
		t.Fatalf("recovered path = (%+v, ready=%v), want new session", session, port.isReady(ready))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) < 2 || sent[0].frame.Kind != turntf.StreamFrameResume || sent[0].session != newSession || sent[1].frame.Kind != turntf.StreamFrameData || sent[1].session != newSession {
		t.Fatalf("recovery frames = %+v", sent)
	}
}

func TestRunStreamTriesNextSessionAfterOpenSendError(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 2, UserID: 3}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	failedSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "failed"}
	currentSession := turntf.SessionRef{ServingNodeID: 5, SessionID: "current"}
	r := &Runtime{
		cfg:           Config{Transport: TransportConfig{SendQueueSize: 4}},
		activeStreams: make(map[turntf.UserRef]*streamPort),
		streamPorts:   make(map[turntf.UserRef]*streamPort),
		streamRecv:    make(map[turntf.StreamID]*streamReceiver),
		streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{
				{Session: failedSession, TransientCapable: true},
				{Session: currentSession, TransientCapable: true},
			}}, nil
		},
		streamNewID: func() (turntf.StreamID, error) { return turntf.StreamID{8}, nil },
	}
	r.streamSend = func(ctx context.Context, _ turntf.UserRef, target turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
		if target == failedSession {
			return turntf.RelayAccepted{}, errors.New("stale session")
		}
		runtimeHandler{runtime: r}.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: currentSession}, turntf.StreamFrame{Kind: turntf.StreamFrameOpenAck, ID: frame.ID, Epoch: frame.Epoch, Window: frame.Window})
		return turntf.RelayAccepted{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan streamRunResult, 1)
	go func() {
		result <- r.runStream(ctx, peer, cancel, func() {})
	}()
	select {
	case got := <-result:
		if got != streamRunStopped {
			t.Fatalf("runStream result = %v, want stopped", got)
		}
	case <-time.After(time.Second):
		t.Fatal("runStream did not activate the second candidate")
	}
}

func TestResumeUpdatesReceiverTargetSession(t *testing.T) {
	peer := turntf.UserRef{NodeID: 2, UserID: 3}
	oldSession := turntf.SessionRef{ServingNodeID: 4, SessionID: "old"}
	newSession := turntf.SessionRef{ServingNodeID: 5, SessionID: "new"}
	id := turntf.StreamID{9}
	receiver := &streamReceiver{peer: peer, targetSession: oldSession, state: turntf.NewStreamReceiverState(id, 1, turntf.DefaultStreamWindow)}
	outbound := testStreamPort()
	outbound.peer = peer
	outbound.targetSession = oldSession
	outbound.readyState = true
	close(outbound.ready)
	var sent sentStreamFrame
	r := &Runtime{
		streamPorts:      map[turntf.UserRef]*streamPort{peer: outbound},
		activeStreams:    map[turntf.UserRef]*streamPort{peer: outbound},
		streamRecv:       map[turntf.StreamID]*streamReceiver{id: receiver},
		preferredStreams: map[turntf.UserRef]turntf.SessionRef{peer: oldSession},
		streamSend: func(_ context.Context, _ turntf.UserRef, target turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			sent = sentStreamFrame{session: target, frame: frame}
			return turntf.RelayAccepted{}, nil
		},
	}
	runtimeHandler{runtime: r}.OnStream(context.Background(), turntf.Packet{Sender: peer, TargetSession: newSession}, turntf.StreamFrame{Kind: turntf.StreamFrameResume, ID: id, Epoch: 2})
	if sent.frame.Kind != turntf.StreamFrameAck || sent.session != newSession {
		t.Fatalf("Resume response = %+v, want Ack to new session", sent)
	}
	if got := receiver.session(); got != newSession {
		t.Fatalf("receiver session = %+v, want %+v", got, newSession)
	}
	if got := r.preferredStreams[peer]; got != newSession {
		t.Fatalf("preferred session = %+v, want %+v", got, newSession)
	}
	select {
	case <-outbound.lost:
	default:
		t.Fatal("Resume from a new session did not invalidate outbound path")
	}
	if outbound.readyState || r.activeStreams[peer] != nil {
		t.Fatal("outbound path remained active after peer session changed")
	}
	select {
	case <-outbound.restart:
		t.Fatal("peer session change replaced the logical stream instead of resuming it")
	default:
	}
	select {
	case <-outbound.done:
		t.Fatal("peer session change closed the logical stream")
	default:
	}
}

func TestReciprocalStreamOpenWithUnchangedSessionDoesNotRestart(t *testing.T) {
	peer := turntf.UserRef{NodeID: 2, UserID: 3}
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "same-session"}
	port := testStreamPort()
	port.peer = peer
	port.targetSession = session
	var mu sync.Mutex
	var sent []sentStreamFrame
	r := &Runtime{
		streamPorts: map[turntf.UserRef]*streamPort{peer: port},
		streamRecv:  make(map[turntf.StreamID]*streamReceiver),
		streamSend: func(_ context.Context, _ turntf.UserRef, target turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			mu.Lock()
			sent = append(sent, sentStreamFrame{session: target, frame: frame})
			mu.Unlock()
			return turntf.RelayAccepted{}, nil
		},
	}
	incomingID := turntf.StreamID{9}
	runtimeHandler{runtime: r}.OnStream(context.Background(), turntf.Packet{Sender: peer, TargetSession: session}, turntf.StreamFrame{Kind: turntf.StreamFrameOpen, ID: incomingID, Epoch: 1, Window: 1024})

	select {
	case <-port.done:
		t.Fatal("unchanged peer session closed outbound port")
	default:
	}
	select {
	case <-port.restart:
		t.Fatal("unchanged peer session requested outbound restart")
	default:
	}
	r.streamMu.RLock()
	receiver := r.streamRecv[incomingID]
	r.streamMu.RUnlock()
	if receiver == nil || receiver.targetSession != session {
		t.Fatalf("receiver = %+v, want session %+v", receiver, session)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 1 || sent[0].frame.Kind != turntf.StreamFrameOpenAck || sent[0].frame.ID != incomingID || sent[0].session != session {
		t.Fatalf("sent frames = %+v, want one OpenAck to reciprocal session", sent)
	}
}

func TestStreamOpenSessionChangeRebuildsOutboundOnce(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 20, UserID: 30}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	oldSession := turntf.SessionRef{ServingNodeID: 40, SessionID: "old-session"}
	newSession := turntf.SessionRef{ServingNodeID: 40, SessionID: "new-session"}
	var mu sync.Mutex
	resolvedSession := oldSession
	resolveCount := 0
	idCount := byte(0)
	var sent []sentStreamFrame
	sentCh := make(chan sentStreamFrame, 16)
	r := &Runtime{
		cfg:         Config{Transport: TransportConfig{SendQueueSize: 4, DialRetryInterval: Duration{Duration: time.Millisecond}}},
		streamPorts: make(map[turntf.UserRef]*streamPort),
		streamRecv:  make(map[turntf.StreamID]*streamReceiver),
		streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			mu.Lock()
			defer mu.Unlock()
			resolveCount++
			return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{{Session: resolvedSession, TransientCapable: true}}}, nil
		},
		streamNewID: func() (turntf.StreamID, error) {
			mu.Lock()
			defer mu.Unlock()
			idCount++
			return turntf.StreamID{idCount}, nil
		},
		relayDial: func(ctx context.Context, _ PeerConfig) {
			<-ctx.Done()
		},
		streamSend: func(_ context.Context, _ turntf.UserRef, target turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			event := sentStreamFrame{session: target, frame: frame}
			mu.Lock()
			sent = append(sent, event)
			mu.Unlock()
			sentCh <- event
			return turntf.RelayAccepted{}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		r.streamLoop(ctx, peer)
		close(loopDone)
	}()
	defer func() {
		cancel()
		select {
		case <-loopDone:
		case <-time.After(time.Second):
			t.Fatal("stream lifecycle did not stop")
		}
	}()

	firstOpen := waitForStreamFrame(t, sentCh, turntf.StreamFrameOpen)
	if firstOpen.session != oldSession {
		t.Fatalf("first Open session = %+v, want %+v", firstOpen.session, oldSession)
	}
	r.streamMu.RLock()
	oldPort := r.streamPorts[peerRef]
	r.streamMu.RUnlock()
	if oldPort == nil || oldPort.id != firstOpen.frame.ID {
		t.Fatalf("old port = %+v, want stream %v", oldPort, firstOpen.frame.ID)
	}
	runtimeHandler{runtime: r}.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: oldSession}, turntf.StreamFrame{Kind: turntf.StreamFrameOpenAck, ID: firstOpen.frame.ID, Epoch: 1, Window: turntf.DefaultStreamWindow})

	mu.Lock()
	resolvedSession = newSession
	mu.Unlock()
	incomingID := turntf.StreamID{99}
	incomingOpen := turntf.StreamFrame{Kind: turntf.StreamFrameOpen, ID: incomingID, Epoch: 1, Window: turntf.DefaultStreamWindow}
	runtimeHandler{runtime: r}.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: newSession}, incomingOpen)
	select {
	case <-oldPort.done:
	case <-time.After(time.Second):
		t.Fatal("old outbound port was not closed")
	}

	secondOpen := waitForStreamFrame(t, sentCh, turntf.StreamFrameOpen)
	if secondOpen.session != newSession {
		t.Fatalf("rebuilt Open session = %+v, want %+v", secondOpen.session, newSession)
	}
	if secondOpen.frame.ID == firstOpen.frame.ID {
		t.Fatalf("rebuilt stream reused ID %v", secondOpen.frame.ID)
	}
	runtimeHandler{runtime: r}.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: newSession}, turntf.StreamFrame{Kind: turntf.StreamFrameOpenAck, ID: secondOpen.frame.ID, Epoch: 1, Window: turntf.DefaultStreamWindow})
	runtimeHandler{runtime: r}.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: newSession}, incomingOpen)
	time.Sleep(25 * time.Millisecond)

	r.streamMu.RLock()
	receiver := r.streamRecv[incomingID]
	currentPort := r.streamPorts[peerRef]
	r.streamMu.RUnlock()
	if receiver == nil || receiver.targetSession != newSession {
		t.Fatalf("new receiver = %+v, want session %+v", receiver, newSession)
	}
	if currentPort == nil || currentPort.id != secondOpen.frame.ID {
		t.Fatalf("current port = %+v, want rebuilt stream %v", currentPort, secondOpen.frame.ID)
	}
	mu.Lock()
	defer mu.Unlock()
	openCount := 0
	for _, event := range sent {
		if event.frame.Kind == turntf.StreamFrameOpen {
			openCount++
		}
	}
	if resolveCount != 1 || openCount != 2 {
		t.Fatalf("resolve count = %d, Open count = %d, want 1 and 2; observed inbound session should be reused", resolveCount, openCount)
	}
}

func waitForStreamFrame(t *testing.T, frames <-chan sentStreamFrame, kind turntf.StreamFrameKind) sentStreamFrame {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case frame := <-frames:
			if frame.frame.Kind == kind {
				return frame
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for stream frame kind %d", kind)
		}
	}
}
