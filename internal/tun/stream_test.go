package tun

import (
	"testing"

	turntf "github.com/tursom/turntf-go"
)

func testStreamPort() *streamPort {
	id := turntf.StreamID{1, 2, 3}
	return &streamPort{
		id: id, sender: turntf.NewStreamSenderState(id, 1, 1024),
		queue: make(chan []byte, 4), ready: make(chan struct{}), lost: make(chan struct{}, 1),
		epoch: 1, openErr: make(chan error, 1), done: make(chan struct{}),
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
