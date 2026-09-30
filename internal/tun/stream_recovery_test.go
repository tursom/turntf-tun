package tun

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	turntf "github.com/tursom/turntf-go"
)

// A peer that restarted no longer knows the old stream ID. Exhausting Resume
// candidates must release runStream so it can Open a new ID, even when the
// process context stays alive and the Relay fallback remains usable.
func TestRecoverStreamReturnsAfterCandidatesExhausted(t *testing.T) {
	for _, failure := range []string{"resolve", "no-session", "resume-send", "resume-rejected", "resume-timeout", "replay-send"} {
		t.Run(failure, func(t *testing.T) {
			peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: 2, UserID: 3}}
			session := turntf.SessionRef{ServingNodeID: 4, SessionID: "restarted"}
			port := testStreamPort()
			defer port.close()
			port.peer = peer.User.ToTurntf()
			if _, err := port.sender.Data([]byte("pending")); err != nil {
				t.Fatal(err)
			}
			var resolves atomic.Int32
			var resumes atomic.Int32
			r := &Runtime{
				cfg: Config{Transport: TransportConfig{
					DialRetryInterval: Duration{Duration: 10 * time.Second},
				}},
				streamTimeout: time.Millisecond,
				streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
					resolves.Add(1)
					if failure == "resolve" {
						return turntf.ResolvedUserSessions{}, errors.New("disconnected")
					}
					if failure == "no-session" {
						return turntf.ResolvedUserSessions{}, nil
					}
					return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{
						{Session: session, TransientCapable: true},
					}}, nil
				},
				streamSend: func(_ context.Context, _ turntf.UserRef, target turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
					if frame.Kind == turntf.StreamFrameResume {
						resumes.Add(1)
						if failure == "resume-rejected" {
							port.failPath(target, frame.Epoch, errors.New("asynchronous rejection"))
							return turntf.RelayAccepted{}, nil
						}
						if failure == "resume-timeout" {
							return turntf.RelayAccepted{}, nil
						}
						if failure == "replay-send" {
							port.acknowledge(target, turntf.StreamFrame{Kind: turntf.StreamFrameAck,
								ID: port.id, Epoch: frame.Epoch, Window: turntf.DefaultStreamWindow})
							return turntf.RelayAccepted{}, nil
						}
					}
					return turntf.RelayAccepted{}, errors.New("old stream state unavailable")
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan bool, 1)
			completed := false
			go func() { done <- r.recoverStream(ctx, peer, port) }()
			defer func() {
				cancel()
				if completed {
					return
				}
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("recovery did not stop after cancellation")
				}
			}()
			select {
			case ok := <-done:
				completed = true
				if ok || port.readyState {
					t.Fatal("failed recovery activated the old stream")
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatal("exhausted candidates kept retrying the old stream")
			}
			if got := resolves.Load(); got != 1 {
				t.Fatalf("resolve calls = %d, want one recovery round", got)
			}
			wantResumes := int32(1)
			if failure == "resolve" || failure == "no-session" {
				wantResumes = 0
			}
			if got := resumes.Load(); got != wantResumes {
				t.Fatalf("Resume calls = %d, want %d", got, wantResumes)
			}
		})
	}
}

func TestStreamLoopRebuildsLostStreamAndKeepsWarmFallback(t *testing.T) {
	peerRef := turntf.UserRef{NodeID: 2, UserID: 3}
	peer := PeerConfig{Name: "peer", User: UserRefConfig{NodeID: peerRef.NodeID, UserID: peerRef.UserID}}
	session := turntf.SessionRef{ServingNodeID: 4, SessionID: "restarted"}
	var ids atomic.Uint32
	var dials atomic.Int32
	var resumes atomic.Int32
	fallback := &peerPort{queue: make(chan []byte, 1), done: make(chan struct{})}
	r := &Runtime{
		cfg: Config{Transport: TransportConfig{SendQueueSize: 4,
			DialRetryInterval: Duration{Duration: time.Millisecond}}},
		localUser:     turntf.UserRef{NodeID: 1, UserID: 1},
		ports:         make(map[turntf.UserRef]*peerPort),
		activeStreams: make(map[turntf.UserRef]*streamPort),
		streamPorts:   make(map[turntf.UserRef]*streamPort),
		streamRecv:    make(map[turntf.StreamID]*streamReceiver),
		streamTimeout: time.Millisecond,
		streamResolve: func(context.Context, turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			return turntf.ResolvedUserSessions{Sessions: []turntf.ResolvedSession{
				{Session: session, TransientCapable: true},
			}}, nil
		},
		streamNewID: func() (turntf.StreamID, error) { return turntf.StreamID{byte(ids.Add(1))}, nil },
	}
	r.streamSend = func(ctx context.Context, _ turntf.UserRef, _ turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
		if frame.Kind == turntf.StreamFrameResume {
			resumes.Add(1)
			// The real receiver silently ignores Resume for an unknown stream ID.
			return turntf.RelayAccepted{}, nil
		}
		if frame.Kind == turntf.StreamFrameOpen {
			runtimeHandler{runtime: r}.OnStream(ctx, turntf.Packet{Sender: peerRef, TargetSession: session},
				turntf.StreamFrame{Kind: turntf.StreamFrameOpenAck, ID: frame.ID, Epoch: frame.Epoch, Window: frame.Window})
		}
		return turntf.RelayAccepted{}, nil
	}
	r.relayDial = func(ctx context.Context, _ PeerConfig) {
		dials.Add(1)
		r.mu.Lock()
		r.ports[peerRef] = fallback
		r.mu.Unlock()
		<-ctx.Done()
		fallback.close()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.streamLoop(ctx, peer); close(done) }()
	defer func() { cancel(); waitForDone(t, done, "stream lifecycle did not stop") }()
	var old *streamPort
	waitForCondition(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		old = r.activeStreams[peerRef]
		return old != nil && r.ports[peerRef] == fallback
	}, "initial stream and warm fallback did not activate")
	old.markPathLost()
	waitForCondition(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		current := r.activeStreams[peerRef]
		return current != nil && current != old && current.id != old.id
	}, "lost remote stream was never replaced with a new ID")
	if resumes.Load() != 1 || dials.Load() != 1 {
		t.Fatalf("Resume attempts = %d, fallback dials = %d, want one each", resumes.Load(), dials.Load())
	}
	select {
	case <-old.done:
	default:
		t.Fatal("old stream was not closed")
	}
	select {
	case <-fallback.done:
		t.Fatal("rebuilding a stream closed its warm Relay fallback")
	default:
	}
}

func TestIdleStreamDetectsMissingAcknowledgementBelowWindow(t *testing.T) {
	port := testStreamPort()
	defer port.close()
	port.targetSession = turntf.SessionRef{ServingNodeID: 4, SessionID: "silent"}
	port.sender = turntf.NewStreamSenderState(port.id, 1, turntf.DefaultStreamWindow)
	port.readyState = true
	close(port.ready)
	sent := make(chan sentStreamFrame, 1)
	r := &Runtime{
		streamTimeout: 5 * time.Millisecond,
		streamSend: func(_ context.Context, _ turntf.UserRef, _ turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			sent <- sentStreamFrame{frame: frame} // Transport accepts the small packet, but no peer ACK returns.
			return turntf.RelayAccepted{}, nil
		},
	}
	done := make(chan struct{})
	go func() { r.writeStreamLoop(port); close(done) }()
	defer func() { port.close(); waitForDone(t, done, "idle stream writer did not stop") }()
	port.queue <- []byte("one-small-packet")
	waitForStreamFrame(t, sent, turntf.StreamFrameData)
	select {
	case <-port.lost:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("idle stream below its window never detected the missing ACK")
	}
	_, ready := port.currentPath()
	if port.isReady(ready) {
		t.Fatal("unacknowledged idle stream remained ready")
	}
}

func TestIdleStreamWithAcknowledgedDataStaysReady(t *testing.T) {
	port := testStreamPort()
	defer port.close()
	port.targetSession = turntf.SessionRef{ServingNodeID: 4, SessionID: "healthy"}
	port.readyState = true
	close(port.ready)
	sent := make(chan struct{}, 1)
	r := &Runtime{
		streamTimeout: 5 * time.Millisecond,
		streamSend: func(_ context.Context, _ turntf.UserRef, session turntf.SessionRef, frame turntf.StreamFrame, _ turntf.DeliveryMode) (turntf.RelayAccepted, error) {
			port.acknowledge(session, turntf.StreamFrame{Kind: turntf.StreamFrameAck,
				ID: frame.ID, Epoch: frame.Epoch, Offset: frame.Offset + uint64(len(frame.Payload)), Window: turntf.DefaultStreamWindow})
			sent <- struct{}{}
			return turntf.RelayAccepted{}, nil
		},
	}
	done := make(chan struct{})
	go func() { r.writeStreamLoop(port); close(done) }()
	defer func() { port.close(); waitForDone(t, done, "healthy stream writer did not stop") }()
	port.queue <- []byte("small-packet")
	waitForDone(t, sent, "small packet was not sent")
	select {
	case <-port.lost:
		t.Fatal("idle stream with all data acknowledged was invalidated")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestStreamSessionResolutionHasRequestDeadline(t *testing.T) {
	r := &Runtime{
		cfg: Config{Turntf: TurntfConfig{RequestTimeout: Duration{Duration: 5 * time.Millisecond}}},
		streamResolve: func(ctx context.Context, _ turntf.UserRef) (turntf.ResolvedUserSessions, error) {
			<-ctx.Done() // Connection stays alive, but the RPC response never arrives.
			return turntf.ResolvedUserSessions{}, ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := r.resolveStreamSessions(ctx, turntf.UserRef{NodeID: 2, UserID: 3}); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			t.Fatalf("resolution error = %v, parent error = %v, want only attempt deadline", err, ctx.Err())
		}
	case <-time.After(100 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("session resolution ignored configured request timeout")
	}
}
