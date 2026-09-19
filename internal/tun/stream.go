package tun

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	turntf "github.com/tursom/turntf-go"
)

const (
	streamOpenTimeout = 5 * time.Second
	streamBatchWait   = 0
	streamBatchMax    = 64 << 10
)

var streamPacketMagic = [2]byte{0x54, 0x50}

func encodeStreamBatch(first []byte, queue <-chan []byte) []byte {
	batch := make([]byte, 0, len(first)+8)
	batch = append(batch, streamPacketMagic[:]...)
	appendPacket := func(packet []byte) bool {
		if len(packet) > 0xffff || len(batch)+2+len(packet) > streamBatchMax {
			return false
		}
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(packet)))
		batch = append(batch, length[:]...)
		batch = append(batch, packet...)
		return true
	}
	if !appendPacket(first) {
		return first
	}
	if streamBatchWait <= 0 {
		for {
			select {
			case packet := <-queue:
				if !appendPacket(packet) {
					return batch
				}
			default:
				return batch
			}
		}
	}
	timer := time.NewTimer(streamBatchWait)
	defer timer.Stop()
	for {
		select {
		case packet := <-queue:
			if !appendPacket(packet) {
				return batch
			}
			if len(batch) >= streamBatchMax-2048 {
				return batch
			}
		case <-timer.C:
			return batch
		}
	}
}

func decodeStreamBatch(payload []byte) ([][]byte, bool) {
	if len(payload) < len(streamPacketMagic) || payload[0] != streamPacketMagic[0] || payload[1] != streamPacketMagic[1] {
		return nil, false
	}
	var packets [][]byte
	for offset := len(streamPacketMagic); offset < len(payload); {
		if len(payload)-offset < 2 {
			return nil, false
		}
		length := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if length == 0 || length > len(payload)-offset {
			return nil, false
		}
		packets = append(packets, payload[offset:offset+length])
		offset += length
	}
	return packets, len(packets) > 0
}

type streamPort struct {
	runtime       *Runtime
	peer          turntf.UserRef
	targetSession turntf.SessionRef
	id            turntf.StreamID
	sender        *turntf.StreamSenderState
	queue         chan []byte
	ready         chan struct{}
	readyState    bool
	lost          chan struct{}
	resumeReady   chan struct{}
	epoch         uint64
	lastAck       uint64
	pathMu        sync.Mutex
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
	if err != nil {
		select {
		case p.openErr <- err:
		default:
		}
		return
	}
	p.pathMu.Lock()
	if !p.readyState {
		p.readyState = true
		close(p.ready)
	}
	p.pathMu.Unlock()
}
func (p *streamPort) markPathLost() {
	p.pathMu.Lock()
	if p.readyState {
		p.readyState = false
		p.ready = make(chan struct{})
	}
	p.pathMu.Unlock()
	select {
	case p.lost <- struct{}{}:
	default:
	}
}
func (p *streamPort) beginResume(session turntf.SessionRef, epoch uint64) (uint64, chan struct{}) {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	p.targetSession = session
	p.epoch = epoch
	p.readyState = false
	p.ready = make(chan struct{})
	p.resumeReady = make(chan struct{}, 1)
	return p.lastAck, p.resumeReady
}
func (p *streamPort) setOpenSession(session turntf.SessionRef) {
	p.pathMu.Lock()
	p.targetSession = session
	p.pathMu.Unlock()
}
func (p *streamPort) currentPath() (turntf.SessionRef, <-chan struct{}) {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	return p.targetSession, p.ready
}
func (p *streamPort) nextEpoch() uint64 {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	return p.epoch + 1
}
func (p *streamPort) isReady(channel <-chan struct{}) bool {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	return p.readyState && p.ready == channel
}
func (p *streamPort) acknowledge(frame turntf.StreamFrame) {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	if frame.Epoch != p.epoch {
		return
	}
	_ = p.sender.Acknowledge(frame.Epoch, frame.Offset, frame.Window)
	if frame.Offset > p.lastAck {
		p.lastAck = frame.Offset
	}
	if p.resumeReady != nil {
		p.readyState = true
		close(p.ready)
		select {
		case p.resumeReady <- struct{}{}:
		default:
		}
		p.resumeReady = nil
	}
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
	port := &streamPort{runtime: r, peer: target, id: id, targetSession: targetSession, sender: turntf.NewStreamSenderState(id, 1, turntf.DefaultStreamWindow), queue: make(chan []byte, r.cfg.Transport.SendQueueSize), ready: make(chan struct{}), lost: make(chan struct{}, 1), epoch: 1, openErr: make(chan error, 1), done: make(chan struct{})}
	r.streamMu.Lock()
	old := r.streamPorts[target]
	r.streamPorts[target] = port
	r.streamMu.Unlock()
	defer func() {
		r.streamMu.Lock()
		if r.streamPorts[target] == port {
			delete(r.streamPorts, target)
		}
		r.streamMu.Unlock()
		port.close()
	}()
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
		r.logf("stream peer %s open rejected", peer.Name)
		port.close()
		r.fallbackRelay(ctx, peer)
		return
	case <-time.After(streamOpenTimeout):
		r.logf("stream peer %s open timeout", peer.Name)
		port.close()
		r.fallbackRelay(ctx, peer)
		return
	case <-port.ready:
	case <-ctx.Done():
		return
	}
	go r.writeStreamLoop(port)
	for {
		select {
		case <-ctx.Done():
			port.close()
			return
		case <-port.done:
			return
		case <-port.lost:
			r.recoverStream(ctx, peer, port)
		}
	}
}

func (r *Runtime) recoverStream(ctx context.Context, peer PeerConfig, p *streamPort) {
	for ctx.Err() == nil {
		sessions, err := r.client.ResolveUserSessions(ctx, p.peer)
		if err == nil {
			for _, candidate := range sessions.Sessions {
				if !candidate.TransientCapable {
					continue
				}
				epoch := p.nextEpoch()
				lastAck, resumeReady := p.beginResume(candidate.Session, epoch)
				frames, resumeErr := p.sender.Resume(epoch, lastAck)
				if resumeErr != nil {
					p.markPathLost()
					break
				}
				resume := turntf.StreamFrame{Kind: turntf.StreamFrameResume, ID: p.id, Epoch: epoch, Offset: lastAck}
				if _, err = r.client.SendStreamFrame(ctx, p.peer, candidate.Session, resume, turntf.DeliveryModeRouteRetry); err != nil {
					p.markPathLost()
					break
				}
				select {
				case <-resumeReady:
					for _, frame := range frames {
						if _, err = r.client.SendStreamFrame(ctx, p.peer, candidate.Session, frame, turntf.DeliveryModeRouteRetry); err != nil {
							p.markPathLost()
							break
						}
					}
					if err == nil {
						return
					}
				case <-time.After(streamOpenTimeout):
					p.markPathLost()
				}
				break
			}
		}
		if !sleepContext(ctx, r.cfg.Transport.DialRetryInterval.Duration) {
			return
		}
	}
}

func (r *Runtime) writeStreamLoop(p *streamPort) {
	var pending []byte
	for {
		session, ready := p.currentPath()
		select {
		case <-p.done:
			return
		case <-ready:
		}
		if !p.isReady(ready) {
			continue
		}
		if pending == nil {
			select {
			case <-p.done:
				return
			case <-ready:
				continue
			case first := <-p.queue:
				pending = encodeStreamBatch(first, p.queue)
			}
		}
		frame, err := p.sender.Data(pending)
		if err != nil {
			if err == turntf.ErrStreamWindowFull {
				time.Sleep(time.Millisecond)
				continue
			}
			p.markPathLost()
			continue
		}
		pending = nil
		if _, err = r.client.SendStreamFrame(context.Background(), p.peer, session, frame, turntf.DeliveryModeRouteRetry); err != nil {
			p.markPathLost()
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
		r.logf("stream open received from %d:%d epoch=%d", packet.Sender.NodeID, packet.Sender.UserID, frame.Epoch)
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
		r.logf("stream open acknowledged by %d:%d epoch=%d", packet.Sender.NodeID, packet.Sender.UserID, frame.Epoch)
		r.streamMu.RLock()
		port := r.streamPorts[packet.Sender]
		r.streamMu.RUnlock()
		if port != nil {
			port.setOpenSession(packet.TargetSession)
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
			if packets, batched := decodeStreamBatch(payload); batched {
				for _, packetPayload := range packets {
					_ = r.device.WritePacket(packetPayload)
				}
			} else {
				_ = r.device.WritePacket(payload)
			}
			r.writeMu.Unlock()
		}
		_, _ = r.client.SendStreamFrame(ctx, receiver.peer, receiver.targetSession, ack, turntf.DeliveryModeRouteRetry)
	case turntf.StreamFrameAck:
		r.streamMu.RLock()
		port := r.streamPorts[packet.Sender]
		r.streamMu.RUnlock()
		if port != nil {
			port.acknowledge(frame)
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
