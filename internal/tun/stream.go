package tun

import (
	"context"
	"encoding/binary"
	"errors"
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

var errStreamAckStalled = errors.New("stream acknowledgement stalled")

func encodeStreamBatch(first []byte, queue <-chan []byte) ([]byte, []byte) {
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
		return first, nil
	}
	if streamBatchWait <= 0 {
		for {
			select {
			case packet := <-queue:
				if !appendPacket(packet) {
					return batch, packet
				}
			default:
				return batch, nil
			}
		}
	}
	timer := time.NewTimer(streamBatchWait)
	defer timer.Stop()
	for {
		select {
		case packet := <-queue:
			if !appendPacket(packet) {
				return batch, packet
			}
			if len(batch) >= streamBatchMax-2048 {
				return batch, nil
			}
		case <-timer.C:
			return batch, nil
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
	lastWindow    uint64
	pathMu        sync.Mutex
	attemptErr    chan error
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	restart       chan struct{}
	once          sync.Once
	restartOnce   sync.Once
	queuedOnce    sync.Once
	sentOnce      sync.Once
}

func (p *streamPort) enqueue(packet []byte) bool {
	select {
	case p.queue <- append([]byte(nil), packet...):
		p.queuedOnce.Do(func() {
			if p.runtime != nil {
				p.runtime.logf("stream packet queued for %d:%d", p.peer.NodeID, p.peer.UserID)
			}
		})
		return true
	default:
		return false
	}
}

func (p *streamPort) enqueueReady(packet []byte) bool {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	if !p.readyState {
		return false
	}
	return p.enqueue(packet)
}
func (p *streamPort) dataForReadyPath(payload []byte) (turntf.SessionRef, turntf.StreamFrame, error, bool) {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	if !p.readyState {
		return turntf.SessionRef{}, turntf.StreamFrame{}, nil, false
	}
	frame, err := p.sender.Data(payload)
	return p.targetSession, frame, err, true
}

func (p *streamPort) ackProgress(session turntf.SessionRef) (uint64, uint64, uint64, bool) {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	if !p.readyState || p.targetSession != session {
		return 0, 0, 0, false
	}
	return p.epoch, p.lastAck, p.lastWindow, true
}

func (p *streamPort) beginOpen(session turntf.SessionRef) (<-chan struct{}, <-chan error) {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	p.targetSession = session
	p.readyState = false
	p.ready = make(chan struct{})
	p.resumeReady = nil
	p.attemptErr = make(chan error, 1)
	return p.ready, p.attemptErr
}
func (p *streamPort) acceptOpenAck(session turntf.SessionRef, epoch uint64) bool {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	if p.targetSession != session || p.epoch != epoch || p.readyState {
		return false
	}
	p.readyState = true
	close(p.ready)
	return true
}
func (p *streamPort) failPath(session turntf.SessionRef, epoch uint64, err error) bool {
	p.pathMu.Lock()
	if p.targetSession != session || p.epoch != epoch {
		p.pathMu.Unlock()
		return false
	}
	if !p.readyState {
		select {
		case p.attemptErr <- err:
		default:
		}
		p.pathMu.Unlock()
		return true
	}
	p.readyState = false
	p.ready = make(chan struct{})
	p.pathMu.Unlock()
	p.signalPathLost()
	return true
}

func (p *streamPort) failPathIfAckUnchanged(session turntf.SessionRef, epoch, expectedAck, expectedWindow uint64, err error) bool {
	p.pathMu.Lock()
	if p.targetSession != session || p.epoch != epoch || p.lastAck != expectedAck || p.lastWindow != expectedWindow {
		p.pathMu.Unlock()
		return false
	}
	if !p.readyState {
		select {
		case p.attemptErr <- err:
		default:
		}
		p.pathMu.Unlock()
		return true
	}
	p.readyState = false
	p.ready = make(chan struct{})
	p.pathMu.Unlock()
	p.signalPathLost()
	return true
}

func (p *streamPort) markPathLost() {
	p.markPathLostWithError(nil)
}

func (p *streamPort) markPathLostWithError(err error) {
	p.pathMu.Lock()
	if !p.readyState {
		if err != nil {
			select {
			case p.attemptErr <- err:
			default:
			}
		}
		p.pathMu.Unlock()
		return
	}
	p.readyState = false
	p.ready = make(chan struct{})
	p.pathMu.Unlock()
	p.signalPathLost()
}

func (p *streamPort) signalPathLost() {
	select {
	case p.lost <- struct{}{}:
	default:
	}
}
func (p *streamPort) beginResume(session turntf.SessionRef, epoch uint64) (uint64, <-chan struct{}, <-chan error) {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	p.targetSession = session
	p.epoch = epoch
	if p.readyState {
		p.readyState = false
		p.ready = make(chan struct{})
	}
	p.resumeReady = make(chan struct{}, 1)
	p.attemptErr = make(chan error, 1)
	return p.lastAck, p.resumeReady, p.attemptErr
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
func (p *streamPort) acknowledge(session turntf.SessionRef, frame turntf.StreamFrame) {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	if session != p.targetSession || frame.Epoch != p.epoch {
		return
	}
	if err := p.sender.Acknowledge(frame.Epoch, frame.Offset, frame.Window); err != nil {
		return
	}
	if frame.Offset > p.lastAck {
		p.lastAck = frame.Offset
	}
	if frame.Window > 0 {
		p.lastWindow = frame.Window
	}
	if p.resumeReady != nil {
		select {
		case p.resumeReady <- struct{}{}:
		default:
		}
		p.resumeReady = nil
	}
}

func (p *streamPort) completeResume(session turntf.SessionRef, epoch uint64) bool {
	p.pathMu.Lock()
	defer p.pathMu.Unlock()
	if p.targetSession != session || p.epoch != epoch || p.readyState || p.resumeReady != nil {
		return false
	}
	select {
	case <-p.attemptErr:
		return false
	default:
	}
	p.readyState = true
	close(p.ready)
	return true
}
func (p *streamPort) requestRestart(session turntf.SessionRef) bool {
	p.pathMu.Lock()
	if p.targetSession == session {
		p.pathMu.Unlock()
		return false
	}
	requested := false
	p.restartOnce.Do(func() {
		close(p.restart)
		requested = true
	})
	p.pathMu.Unlock()
	if requested {
		p.close()
	}
	return requested
}
func (p *streamPort) restartRequested() bool {
	select {
	case <-p.restart:
		return true
	default:
		return false
	}
}
func (p *streamPort) close() {
	p.once.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
		close(p.done)
	})
}

type streamRunResult uint8

const (
	streamRunStopped streamRunResult = iota
	streamRunRestart
	streamRunFallback
)

func (r *Runtime) resolveStreamSessions(ctx context.Context, peer turntf.UserRef) (turntf.ResolvedUserSessions, error) {
	if r.streamResolve != nil {
		return r.streamResolve(ctx, peer)
	}
	return r.client.ResolveUserSessions(ctx, peer)
}

func (r *Runtime) sendStreamFrame(ctx context.Context, peer turntf.UserRef, session turntf.SessionRef, frame turntf.StreamFrame) (turntf.RelayAccepted, error) {
	if r.streamSend != nil {
		return r.streamSend(ctx, peer, session, frame, turntf.DeliveryModeRouteRetry)
	}
	_, err := r.client.SendStreamFrameTracked(ctx, peer, session, frame, turntf.DeliveryModeRouteRetry)
	return turntf.RelayAccepted{}, err
}

func (r *Runtime) newStreamID() (turntf.StreamID, error) {
	if r.streamNewID != nil {
		return r.streamNewID()
	}
	return turntf.NewStreamID()
}

func (r *Runtime) streamAttemptTimeout() time.Duration {
	if r.streamTimeout > 0 {
		return r.streamTimeout
	}
	return streamOpenTimeout
}

func transientStreamSessions(sessions turntf.ResolvedUserSessions) []turntf.SessionRef {
	result := make([]turntf.SessionRef, 0, len(sessions.Sessions))
	seen := make(map[turntf.SessionRef]struct{}, len(sessions.Sessions))
	for _, candidate := range sessions.Sessions {
		if !candidate.TransientCapable || candidate.Session.IsZero() {
			continue
		}
		if _, ok := seen[candidate.Session]; ok {
			continue
		}
		seen[candidate.Session] = struct{}{}
		result = append(result, candidate.Session)
	}
	return result
}

func (r *Runtime) streamLoop(ctx context.Context, peer PeerConfig) {
	var fallbackCancel context.CancelFunc
	var fallbackDone chan struct{}
	startFallback := func() {
		if fallbackDone != nil || !shouldDial(r.localUser, peer.User.ToTurntf(), peer.DialPolicy) {
			return
		}
		fallbackCtx, cancel := context.WithCancel(ctx)
		fallbackCancel = cancel
		fallbackDone = make(chan struct{})
		go func(done chan struct{}) {
			defer close(done)
			r.dialPeer(fallbackCtx, peer)
		}(fallbackDone)
	}
	stopFallback := func() {
		if fallbackDone == nil {
			return
		}
		fallbackCancel()
		<-fallbackDone
		fallbackCancel = nil
		fallbackDone = nil
	}
	startFallback()
	defer stopFallback()
	for ctx.Err() == nil {
		switch r.runStream(ctx, peer, func() {}, func() {}) {
		case streamRunRestart:
			continue
		case streamRunFallback:
			if !sleepContext(ctx, r.cfg.Transport.DialRetryInterval.Duration) {
				return
			}
			continue
		}
		return
	}
}

func (r *Runtime) runStream(ctx context.Context, peer PeerConfig, streamReady, streamLost func()) streamRunResult {
	target := peer.User.ToTurntf()
	r.streamMu.RLock()
	preferredSession := r.preferredStreams[target]
	r.streamMu.RUnlock()
	var candidates []turntf.SessionRef
	if preferredSession.IsZero() {
		sessions, err := r.resolveStreamSessions(ctx, target)
		if err != nil {
			r.logf("resolve stream peer %s: %v", peer.Name, err)
			return streamRunFallback
		}
		candidates = transientStreamSessions(sessions)
	} else {
		candidates = []turntf.SessionRef{preferredSession}
	}
	if len(candidates) == 0 {
		r.logf("stream peer %s has no transient session", peer.Name)
		return streamRunFallback
	}
	id, err := r.newStreamID()
	if err != nil {
		return streamRunFallback
	}
	portCtx, cancel := context.WithCancel(ctx)
	port := &streamPort{runtime: r, peer: target, id: id, sender: turntf.NewStreamSenderState(id, 1, turntf.DefaultStreamWindow), queue: make(chan []byte, r.cfg.Transport.SendQueueSize), ready: make(chan struct{}), lost: make(chan struct{}, 1), epoch: 1, attemptErr: make(chan error, 1), ctx: portCtx, cancel: cancel, done: make(chan struct{}), restart: make(chan struct{})}
	active := false
	r.streamMu.Lock()
	old := r.streamPorts[target]
	r.streamPorts[target] = port
	r.streamMu.Unlock()
	defer func() {
		if active {
			r.deactivateStream(target, port)
		}
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
	opened := false
	for _, candidate := range candidates {
		ready, attemptErr := port.beginOpen(candidate)
		if _, err := r.sendStreamFrame(port.ctx, target, candidate, open); err != nil {
			r.logf("stream peer %s open session=%d/%s failed: %v", peer.Name, candidate.ServingNodeID, candidate.SessionID, err)
			continue
		}
		timer := time.NewTimer(r.streamAttemptTimeout())
		select {
		case err := <-attemptErr:
			timer.Stop()
			r.logf("stream peer %s open session=%d/%s rejected: %v", peer.Name, candidate.ServingNodeID, candidate.SessionID, err)
			continue
		case <-timer.C:
			r.logf("stream peer %s open session=%d/%s timeout", peer.Name, candidate.ServingNodeID, candidate.SessionID)
			continue
		case <-ready:
			timer.Stop()
			opened = true
		case <-port.restart:
			timer.Stop()
			return streamRunRestart
		case <-port.done:
			timer.Stop()
			if port.restartRequested() {
				return streamRunRestart
			}
			return streamRunStopped
		case <-ctx.Done():
			timer.Stop()
			return streamRunStopped
		}
		if opened {
			break
		}
	}
	if !opened {
		if !preferredSession.IsZero() {
			r.streamMu.Lock()
			if r.preferredStreams[target] == preferredSession {
				delete(r.preferredStreams, target)
			}
			r.streamMu.Unlock()
		}
		return streamRunFallback
	}
	r.activateStream(target, port)
	active = true
	streamReady()
	go r.writeStreamLoop(port)
	for {
		select {
		case <-ctx.Done():
			return streamRunStopped
		case <-port.restart:
			return streamRunRestart
		case <-port.done:
			if port.restartRequested() {
				return streamRunRestart
			}
			return streamRunStopped
		case <-port.lost:
			if active {
				r.deactivateStream(target, port)
				active = false
			}
			streamLost()
			if !r.recoverStream(port.ctx, peer, port) {
				if ctx.Err() != nil {
					return streamRunStopped
				}
				return streamRunRestart
			}
			r.activateStream(target, port)
			active = true
			streamReady()
		}
	}
}

func (r *Runtime) activateStream(peer turntf.UserRef, stream *streamPort) {
	r.mu.Lock()
	if r.activeStreams == nil {
		r.activeStreams = make(map[turntf.UserRef]*streamPort)
	}
	r.activeStreams[peer] = stream
	r.mu.Unlock()
}

func (r *Runtime) deactivateStream(peer turntf.UserRef, stream *streamPort) {
	r.mu.Lock()
	if r.activeStreams[peer] == stream {
		delete(r.activeStreams, peer)
	}
	r.mu.Unlock()
}

func (r *Runtime) deactivateStreamIfNotReady(peer turntf.UserRef, stream *streamPort) {
	r.mu.Lock()
	if r.activeStreams[peer] == stream {
		stream.pathMu.Lock()
		ready := stream.readyState
		stream.pathMu.Unlock()
		if !ready {
			delete(r.activeStreams, peer)
		}
	}
	r.mu.Unlock()
}

func (r *Runtime) recoverStream(ctx context.Context, peer PeerConfig, p *streamPort) bool {
	for ctx.Err() == nil {
		sessions, err := r.resolveStreamSessions(ctx, p.peer)
		if err == nil {
			for _, session := range transientStreamSessions(sessions) {
				epoch := p.nextEpoch()
				lastAck, resumeReady, attemptErr := p.beginResume(session, epoch)
				frames, resumeErr := p.sender.Resume(epoch, lastAck)
				if resumeErr != nil {
					return false
				}
				resume := turntf.StreamFrame{Kind: turntf.StreamFrameResume, ID: p.id, Epoch: epoch, Offset: lastAck}
				if _, err = r.sendStreamFrame(ctx, p.peer, session, resume); err != nil {
					continue
				}
				timer := time.NewTimer(r.streamAttemptTimeout())
				select {
				case <-resumeReady:
					timer.Stop()
					for _, frame := range frames {
						if _, err = r.sendStreamFrame(ctx, p.peer, session, frame); err != nil {
							break
						}
					}
					if err == nil && p.completeResume(session, epoch) {
						return true
					}
				case <-attemptErr:
					timer.Stop()
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return false
				}
			}
		}
		if !sleepContext(ctx, r.cfg.Transport.DialRetryInterval.Duration) {
			return false
		}
	}
	return false
}

func (r *Runtime) writeStreamLoop(p *streamPort) {
	var pending []byte
	var overflow []byte
	var stalled streamWindowStall
	for {
		if pending == nil {
			var first []byte
			if overflow != nil {
				first, overflow = overflow, nil
			} else {
				select {
				case <-p.done:
					return
				case first = <-p.queue:
				}
			}
			pending, overflow = encodeStreamBatch(first, p.queue)
		}
		_, ready := p.currentPath()
		select {
		case <-p.done:
			return
		case <-ready:
		}
		session, frame, err, pathReady := p.dataForReadyPath(pending)
		if !pathReady {
			continue
		}
		if err != nil {
			if err == turntf.ErrStreamWindowFull {
				epoch, lastAck, lastWindow, current := p.ackProgress(session)
				if !current {
					stalled.reset()
					continue
				}
				if stalled.observe(session, epoch, lastAck, lastWindow, time.Now(), r.streamAttemptTimeout()) {
					if p.failPathIfAckUnchanged(session, epoch, lastAck, lastWindow, errStreamAckStalled) {
						r.logf("stream acknowledgements from %d:%d session=%d/%s stalled at offset %d", p.peer.NodeID, p.peer.UserID, session.ServingNodeID, session.SessionID, lastAck)
					}
					stalled.reset()
					continue
				}
				if !sleepContext(p.ctx, time.Millisecond) {
					return
				}
				continue
			}
			stalled.reset()
			p.failPath(session, frame.Epoch, err)
			continue
		}
		stalled.reset()
		pending = nil
		if _, err = r.sendStreamFrame(p.ctx, p.peer, session, frame); err != nil {
			p.failPath(session, frame.Epoch, err)
		} else {
			p.sentOnce.Do(func() { r.logf("stream data sent to %d:%d", p.peer.NodeID, p.peer.UserID) })
		}
	}
}

type streamWindowStall struct {
	session turntf.SessionRef
	epoch   uint64
	ack     uint64
	window  uint64
	since   time.Time
}

func (s *streamWindowStall) observe(session turntf.SessionRef, epoch, ack, window uint64, now time.Time, timeout time.Duration) bool {
	if s.since.IsZero() || s.session != session || s.epoch != epoch || s.ack != ack || s.window != window {
		s.session = session
		s.epoch = epoch
		s.ack = ack
		s.window = window
		s.since = now
		return false
	}
	return now.Sub(s.since) >= timeout
}

func (s *streamWindowStall) reset() {
	*s = streamWindowStall{}
}

type streamReceiver struct {
	peer          turntf.UserRef
	targetSession turntf.SessionRef
	state         *turntf.StreamReceiverState
	pathMu        sync.RWMutex
	receivedOnce  sync.Once
}

func (r *streamReceiver) session() turntf.SessionRef {
	r.pathMu.RLock()
	defer r.pathMu.RUnlock()
	return r.targetSession
}

func (r *streamReceiver) setSession(session turntf.SessionRef) {
	if session.IsZero() {
		return
	}
	r.pathMu.Lock()
	r.targetSession = session
	r.pathMu.Unlock()
}

func (r *Runtime) markAllStreamPathsLost(reason error) {
	type peerStream struct {
		peer turntf.UserRef
		port *streamPort
	}
	r.streamMu.RLock()
	ports := make([]peerStream, 0, len(r.streamPorts))
	for peer, port := range r.streamPorts {
		ports = append(ports, peerStream{peer: peer, port: port})
	}
	r.streamMu.RUnlock()
	for _, item := range ports {
		item.port.markPathLostWithError(reason)
		r.deactivateStreamIfNotReady(item.peer, item.port)
	}
	if len(ports) > 0 && reason != nil {
		r.logf("stream transport invalidated %d paths: %v", len(ports), reason)
	}
}

type runtimeHandler struct{ runtime *Runtime }

func (runtimeHandler) OnLogin(context.Context, turntf.LoginInfo) {}
func (runtimeHandler) OnMessage(context.Context, turntf.Message) {}
func (runtimeHandler) OnPacket(context.Context, turntf.Packet)   {}
func (h runtimeHandler) OnError(_ context.Context, err error) {
	if h.runtime != nil {
		h.runtime.logf("turntf client error: %v", err)
	}
}
func (h runtimeHandler) OnDisconnect(_ context.Context, err error) {
	if h.runtime != nil {
		h.runtime.markAllStreamPathsLost(err)
	}
}
func (h runtimeHandler) OnStreamSendResult(_ context.Context, result turntf.StreamSendResult) {
	if h.runtime == nil || result.Err == nil {
		return
	}
	r := h.runtime
	peer := result.Metadata.Target
	failedSession := result.Metadata.TargetSession
	r.streamMu.RLock()
	port := r.streamPorts[peer]
	r.streamMu.RUnlock()
	if port == nil || port.id != result.Metadata.StreamID {
		return
	}
	if !port.failPath(failedSession, result.Metadata.Epoch, result.Err) {
		return
	}
	r.deactivateStreamIfNotReady(peer, port)
	r.streamMu.Lock()
	if r.streamPorts[peer] == port && r.preferredStreams[peer] == failedSession {
		delete(r.preferredStreams, peer)
	}
	r.streamMu.Unlock()
	r.logf("stream send to %d:%d session=%d/%s failed: %v", peer.NodeID, peer.UserID, failedSession.ServingNodeID, failedSession.SessionID, result.Err)
}
func (h runtimeHandler) OnRelayOrphan(_ context.Context, relayID string, kind turntf.RelayKind) {
	if h.runtime != nil {
		h.runtime.logRelayOrphan(relayID, kind)
	}
}
func (h runtimeHandler) OnStream(ctx context.Context, packet turntf.Packet, frame turntf.StreamFrame) {
	if h.runtime == nil {
		return
	}
	r := h.runtime
	switch frame.Kind {
	case turntf.StreamFrameOpen:
		r.logf("stream open received from %d:%d epoch=%d", packet.Sender.NodeID, packet.Sender.UserID, frame.Epoch)
		r.streamMu.Lock()
		for id, existing := range r.streamRecv {
			if id != frame.ID && existing.peer == packet.Sender {
				delete(r.streamRecv, id)
			}
		}
		receiver := r.streamRecv[frame.ID]
		if receiver == nil {
			receiver = &streamReceiver{peer: packet.Sender, targetSession: packet.TargetSession, state: turntf.NewStreamReceiverState(frame.ID, frame.Epoch, frame.Window)}
			r.streamRecv[frame.ID] = receiver
		} else {
			receiver.setSession(packet.TargetSession)
		}
		restart := false
		if !packet.TargetSession.IsZero() {
			if r.preferredStreams == nil {
				r.preferredStreams = make(map[turntf.UserRef]turntf.SessionRef)
			}
			r.preferredStreams[packet.Sender] = packet.TargetSession
			if port := r.streamPorts[packet.Sender]; port != nil {
				session, _ := port.currentPath()
				restart = session != packet.TargetSession && port.requestRestart(packet.TargetSession)
			}
		}
		r.streamMu.Unlock()
		if restart {
			r.logf("stream peer %d:%d observed new session; rebuilding outbound stream", packet.Sender.NodeID, packet.Sender.UserID)
		}
		ack := turntf.StreamFrame{Kind: turntf.StreamFrameOpenAck, ID: frame.ID, Epoch: frame.Epoch, Window: frame.Window}
		_, _ = r.sendStreamFrame(ctx, packet.Sender, packet.TargetSession, ack)
	case turntf.StreamFrameOpenAck:
		r.logf("stream open acknowledged by %d:%d epoch=%d", packet.Sender.NodeID, packet.Sender.UserID, frame.Epoch)
		r.streamMu.RLock()
		port := r.streamPorts[packet.Sender]
		r.streamMu.RUnlock()
		if port != nil && port.id == frame.ID {
			port.acceptOpenAck(packet.TargetSession, frame.Epoch)
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
			receiver.receivedOnce.Do(func() { r.logf("stream data received from %d:%d", receiver.peer.NodeID, receiver.peer.UserID) })
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
		_, _ = r.sendStreamFrame(ctx, receiver.peer, receiver.session(), ack)
	case turntf.StreamFrameAck:
		r.streamMu.RLock()
		port := r.streamPorts[packet.Sender]
		r.streamMu.RUnlock()
		if port != nil && port.id == frame.ID {
			port.acknowledge(packet.TargetSession, frame)
		}
	case turntf.StreamFrameResume:
		r.streamMu.RLock()
		receiver := r.streamRecv[frame.ID]
		r.streamMu.RUnlock()
		if receiver != nil {
			if ack, err := receiver.state.Resume(frame.Epoch, frame.Offset); err == nil {
				receiver.setSession(packet.TargetSession)
				var lostPort *streamPort
				if !packet.TargetSession.IsZero() {
					r.streamMu.Lock()
					if r.preferredStreams == nil {
						r.preferredStreams = make(map[turntf.UserRef]turntf.SessionRef)
					}
					r.preferredStreams[packet.Sender] = packet.TargetSession
					if port := r.streamPorts[packet.Sender]; port != nil {
						session, _ := port.currentPath()
						if session != packet.TargetSession {
							lostPort = port
						}
					}
					r.streamMu.Unlock()
				}
				if lostPort != nil {
					lostPort.markPathLostWithError(errors.New("peer session changed"))
					r.deactivateStreamIfNotReady(packet.Sender, lostPort)
					r.logf("stream peer %d:%d resumed from new session; recovering outbound stream", packet.Sender.NodeID, packet.Sender.UserID)
				}
				_, _ = r.sendStreamFrame(ctx, receiver.peer, receiver.session(), ack)
			}
		}
	case turntf.StreamFrameClose:
		r.streamMu.Lock()
		delete(r.streamRecv, frame.ID)
		r.streamMu.Unlock()
	}
}

var _ turntf.StreamHandler = runtimeHandler{}
var _ turntf.StreamSendHandler = runtimeHandler{}
