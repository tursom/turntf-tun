package tun

import (
	"context"
	"sync"
	"time"

	turntf "github.com/tursom/turntf-go"
)

const streamOpenTimeout = 5 * time.Second

type streamPort struct {
	runtime       *Runtime
	peer          turntf.UserRef
	targetSession turntf.SessionRef
	sender        *turntf.StreamSenderState
	queue         chan []byte
	ready         chan struct{}
	readyOnce     sync.Once
	openErr       chan error
	done          chan struct{}
	once          sync.Once
}

func (p *streamPort) enqueue(packet []byte) bool {
	select {
	case p.queue <- append([]byte(nil), packet...):
		return true
	default:
		return false
	}
}
func (p *streamPort) markReady(err error) {
	p.readyOnce.Do(func() {
		if err != nil {
			p.openErr <- err
		}
		close(p.ready)
	})
}
func (p *streamPort) close() { p.once.Do(func() { close(p.done) }) }

func (r *Runtime) streamLoop(ctx context.Context, peer PeerConfig) {
	target := peer.User.ToTurntf()
	sessions, err := r.client.ResolveUserSessions(ctx, target)
	if err != nil {
		r.logf("resolve stream peer %s: %v", peer.Name, err)
		r.fallbackRelay(ctx, peer)
		return
	}
	var targetSession turntf.SessionRef
	for _, candidate := range sessions.Sessions {
		if candidate.TransientCapable {
			targetSession = candidate.Session
			break
		}
	}
	if targetSession.IsZero() {
		r.logf("stream peer %s has no transient session", peer.Name)
		r.fallbackRelay(ctx, peer)
		return
	}
	id, err := turntf.NewStreamID()
	if err != nil {
		r.fallbackRelay(ctx, peer)
		return
	}
	port := &streamPort{runtime: r, peer: target, targetSession: targetSession, sender: turntf.NewStreamSenderState(id, 1, turntf.DefaultStreamWindow), queue: make(chan []byte, r.cfg.Transport.SendQueueSize), done: make(chan struct{}), ready: make(chan struct{}), openErr: make(chan error, 1)}
	r.streamMu.Lock()
	old := r.streamPorts[target]
	r.streamPorts[target] = port
	r.streamMu.Unlock()
	if old != nil {
		old.close()
	}
	open := turntf.StreamFrame{Kind: turntf.StreamFrameOpen, ID: id, Epoch: 1, Window: turntf.DefaultStreamWindow}
	if _, err := r.client.SendStreamFrame(ctx, target, targetSession, open, turntf.DeliveryModeRouteRetry); err != nil {
		port.close()
		r.fallbackRelay(ctx, peer)
		return
	}
	select {
	case <-port.openErr:
		port.close()
		r.fallbackRelay(ctx, peer)
		return
	case <-time.After(streamOpenTimeout):
		port.close()
		r.fallbackRelay(ctx, peer)
		return
	case <-port.ready:
	case <-ctx.Done():
		return
	}
	go r.writeStreamLoop(port)
	select {
	case <-ctx.Done():
	case <-port.done:
		r.fallbackRelay(ctx, peer)
	}
	r.streamMu.Lock()
	if r.streamPorts[target] == port {
		delete(r.streamPorts, target)
	}
	r.streamMu.Unlock()
}

func (r *Runtime) writeStreamLoop(p *streamPort) {
	for {
		select {
		case <-p.done:
			return
		case packet := <-p.queue:
			frame, err := p.sender.Data(packet)
			if err != nil {
				p.close()
				return
			}
			if _, err = r.client.SendStreamFrame(context.Background(), p.peer, p.targetSession, frame, turntf.DeliveryModeRouteRetry); err != nil {
				p.close()
				return
			}
		}
	}
}

func (r *Runtime) fallbackRelay(ctx context.Context, peer PeerConfig) {
	r.dialLoop(ctx, peer)
}

type streamReceiver struct {
	peer          turntf.UserRef
	targetSession turntf.SessionRef
	state         *turntf.StreamReceiverState
}

type runtimeHandler struct{ runtime *Runtime }

func (runtimeHandler) OnLogin(context.Context, turntf.LoginInfo) {}
func (runtimeHandler) OnMessage(context.Context, turntf.Message) {}
func (runtimeHandler) OnPacket(context.Context, turntf.Packet)   {}
func (runtimeHandler) OnError(context.Context, error)            {}
func (runtimeHandler) OnDisconnect(context.Context, error)       {}
func (h runtimeHandler) OnStream(ctx context.Context, packet turntf.Packet, frame turntf.StreamFrame) {
	if h.runtime == nil {
		return
	}
	r := h.runtime
	switch frame.Kind {
	case turntf.StreamFrameOpen:
		r.streamMu.Lock()
		receiver := r.streamRecv[frame.ID]
		if receiver == nil {
			receiver = &streamReceiver{peer: packet.Sender, targetSession: packet.TargetSession, state: turntf.NewStreamReceiverState(frame.ID, frame.Epoch, frame.Window)}
			r.streamRecv[frame.ID] = receiver
		}
		r.streamMu.Unlock()
		ack := turntf.StreamFrame{Kind: turntf.StreamFrameOpenAck, ID: frame.ID, Epoch: frame.Epoch, Window: frame.Window}
		_, _ = r.client.SendStreamFrame(ctx, packet.Sender, packet.TargetSession, ack, turntf.DeliveryModeRouteRetry)
	case turntf.StreamFrameOpenAck:
		r.streamMu.RLock()
		port := r.streamPorts[packet.Sender]
		r.streamMu.RUnlock()
		if port != nil {
			port.targetSession = packet.TargetSession
			port.markReady(nil)
		}
	case turntf.StreamFrameData:
		r.streamMu.RLock()
		receiver := r.streamRecv[frame.ID]
		r.streamMu.RUnlock()
		if receiver == nil {
			return
		}
		payload, ack, err := receiver.state.Accept(frame)
		if err != nil {
			return
		}
		if len(payload) > 0 {
			r.writeMu.Lock()
			_ = r.device.WritePacket(payload)
			r.writeMu.Unlock()
		}
		_, _ = r.client.SendStreamFrame(ctx, receiver.peer, receiver.targetSession, ack, turntf.DeliveryModeRouteRetry)
	case turntf.StreamFrameAck:
		r.streamMu.RLock()
		port := r.streamPorts[packet.Sender]
		r.streamMu.RUnlock()
		if port != nil {
			_ = port.sender.Acknowledge(frame.Epoch, frame.Offset, frame.Window)
		}
	case turntf.StreamFrameResume:
		r.streamMu.RLock()
		receiver := r.streamRecv[frame.ID]
		r.streamMu.RUnlock()
		if receiver != nil {
			if ack, err := receiver.state.Resume(frame.Epoch, frame.Offset); err == nil {
				_, _ = r.client.SendStreamFrame(ctx, receiver.peer, receiver.targetSession, ack, turntf.DeliveryModeRouteRetry)
			}
		}
	case turntf.StreamFrameClose:
		r.streamMu.Lock()
		delete(r.streamRecv, frame.ID)
		r.streamMu.Unlock()
	}
}

var _ turntf.StreamHandler = runtimeHandler{}
