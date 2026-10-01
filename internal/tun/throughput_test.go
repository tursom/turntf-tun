package tun

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	turntf "github.com/tursom/turntf-go"
)

func TestRelaySendBufferKeepsBackpressureShallow(t *testing.T) {
	// The SDK allocates one send slot per KiB. A deep channel hides a full
	// Relay window from writeRelayLoop, so batches stay one packet deep.
	if slots := relaySendBufferBytes / 1024; slots < 1 || slots > 8 {
		t.Fatalf("relay send channel slots = %d, want 1..8", slots)
	}
}

func TestWriteRelayLoopBatchesBacklogWhileSendBlocks(t *testing.T) {
	conn := newFakeRelayConn("relay")
	conn.sent = make(chan []byte)
	r := &Runtime{}
	port := &peerPort{runtime: r, peer: &peerConfig{config: &PeerConfig{Name: "peer"}}, conn: conn, queue: make(chan []byte, 8), done: make(chan struct{})}
	go r.writeRelayLoop(port)
	defer port.close()

	port.queue <- ipv4Packet(10, 0, 0, 1)
	// The first Send blocks like a full window; packets queued meanwhile must
	// leave as one batch instead of one frame each.
	time.Sleep(20 * time.Millisecond)
	for i := byte(2); i <= 4; i++ {
		port.queue <- ipv4Packet(10, 0, 0, i)
	}
	var frames [][]byte
	for len(frames) < 2 {
		select {
		case frame := <-conn.sent:
			frames = append(frames, frame)
		case <-time.After(time.Second):
			t.Fatalf("received %d relay frames, want 2", len(frames))
		}
	}
	var count int
	if err := decodeTunBatch(frames[1], 65535, func([]byte) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("second relay batch carried %d packets, want 3", count)
	}
}

func readyThroughputStreamPort(window uint64) (*streamPort, turntf.SessionRef) {
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "session"}
	port := testStreamPort()
	port.sender = turntf.NewStreamSenderState(port.id, 1, window)
	port.queue = make(chan []byte, 8)
	port.wake = make(chan struct{}, 1)
	port.targetSession = session
	port.readyState = true
	close(port.ready)
	return port, session
}

func TestStreamWriterAbsorbsBacklogWhileWindowFull(t *testing.T) {
	port, session := readyThroughputStreamPort(4096)
	frames := make(chan turntf.StreamFrame, 8)
	r := &Runtime{streamTimeout: 10 * time.Second, streamSend: func(_ context.Context, _ turntf.UserRef, _ turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
		frames <- frame
		return turntf.RelayAccepted{}, nil
	}}
	go r.writeStreamLoop(port)
	defer port.close()
	next := func() turntf.StreamFrame {
		t.Helper()
		select {
		case frame := <-frames:
			return frame
		case <-time.After(time.Second):
			t.Fatal("stream frame was not sent")
			return turntf.StreamFrame{}
		}
	}

	port.queue <- bytes.Repeat([]byte{'a'}, 3000)
	first := next()
	// B no longer fits the window; C and D arrive while the writer waits.
	port.queue <- bytes.Repeat([]byte{'b'}, 1200)
	time.Sleep(20 * time.Millisecond)
	port.queue <- bytes.Repeat([]byte{'c'}, 1000)
	port.queue <- bytes.Repeat([]byte{'d'}, 1000)
	port.acknowledge(session, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: 1, Offset: first.Offset + uint64(len(first.Payload)), Window: 4096})

	packets, ok := decodeStreamBatch(next().Payload)
	if !ok || len(packets) != 3 || packets[0][0] != 'b' || packets[1][0] != 'c' || packets[2][0] != 'd' {
		t.Fatalf("frame after ACK carried %d packets (ok=%v), want b,c,d in order", len(packets), ok)
	}
}

func TestStreamWindowWaiterWakesOnAckAndPathLoss(t *testing.T) {
	port, session := readyThroughputStreamPort(4096)
	frame, err := port.sender.Data([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	waitWoken := func(trigger func()) {
		t.Helper()
		woken := make(chan bool, 1)
		go func() { woken <- port.waitWriterWake(time.Hour) }()
		trigger()
		select {
		case ok := <-woken:
			if !ok {
				t.Fatal("waiter reported closed port")
			}
		case <-time.After(time.Second):
			t.Fatal("window waiter was not woken")
		}
	}
	waitWoken(func() {
		port.acknowledge(session, turntf.StreamFrame{Kind: turntf.StreamFrameAck, ID: port.id, Epoch: 1, Offset: frame.Offset + uint64(len(frame.Payload)), Window: 4096})
	})
	waitWoken(func() { port.failPath(session, 1, errors.New("lost")) })
}

func TestStreamDataAckDoesNotBlockReader(t *testing.T) {
	peer := turntf.UserRef{NodeID: 2, UserID: 3}
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "session"}
	id := turntf.StreamID{7}
	receiver := &streamReceiver{peer: peer, targetSession: session, state: turntf.NewStreamReceiverState(id, 1, turntf.DefaultStreamWindow)}
	release := make(chan struct{})
	acks := make(chan turntf.StreamFrame, 8)
	r := &Runtime{
		device:     &recordingPacketDevice{writes: make(chan []byte, 8)},
		streamRecv: map[turntf.StreamID]*streamReceiver{id: receiver},
		streamSend: func(_ context.Context, _ turntf.UserRef, _ turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			acks <- frame
			<-release
			return turntf.RelayAccepted{}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := runtimeHandler{runtime: r}
	delivered := make(chan struct{})
	go func() {
		defer close(delivered)
		var offset uint64
		for _, size := range []int{100, 200, 300} {
			handler.OnStream(ctx, turntf.Packet{Sender: peer, TargetSession: session}, turntf.StreamFrame{Kind: turntf.StreamFrameData, ID: id, Epoch: 1, Offset: offset, Payload: bytes.Repeat([]byte{0x45}, size)})
			offset += uint64(size)
		}
	}()
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("blocked ACK write stalled the shared stream reader")
	}
	close(release)
	var last turntf.StreamFrame
	deadline := time.After(time.Second)
	for last.Offset != 600 {
		select {
		case last = <-acks:
		case <-deadline:
			t.Fatalf("last ACK offset = %d, want cumulative 600", last.Offset)
		}
	}
	receiver.stopAcks()
}
