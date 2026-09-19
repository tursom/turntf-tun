package tun

import (
	"context"
	"sync"
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
		epoch: 1, openErr: make(chan error, 1), ctx: ctx, cancel: cancel,
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
	lastAck, ready := p.beginResume(turntf.SessionRef{ServingNodeID: 2, SessionID: "session-3"}, 2)
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
	p.acknowledge(turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: p.id, Epoch: 2, Offset: frame.Offset + uint64(len(frame.Payload))})
	select {
	case <-ready:
	default:
		t.Fatal("resume ACK did not release path")
	}
	if got := <-p.queue; string(got) != "queued" {
		t.Fatalf("queued packet = %q", got)
	}
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
	t.Skip("session changes are handled by outbound OpenAck/send failure, not inbound Open")
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
	if resolveCount != 2 || openCount != 2 {
		t.Fatalf("resolve count = %d, Open count = %d, want 2 and 2", resolveCount, openCount)
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
