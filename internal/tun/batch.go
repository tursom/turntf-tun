package tun

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var tunBatchMagic = [...]byte{0x54, 0x54, 0x55, 0x4e, 0x01}

const (
	tunBatchMaxBytes = 128 << 10
	tunBatchHeader   = len(tunBatchMagic) + 4
)

func appendTunBatch(dst, packet []byte) ([]byte, bool) {
	if len(packet) == 0 || len(packet) > 65535 {
		return dst, false
	}
	if len(dst) == 0 {
		dst = make([]byte, 0, tunBatchMaxBytes)
		dst = append(dst, tunBatchMagic[:]...)
	}
	if len(dst)+4+len(packet) > tunBatchMaxBytes {
		return dst, false
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(packet)))
	dst = append(dst, length[:]...)
	dst = append(dst, packet...)
	return dst, true
}

func decodeTunBatch(frame []byte, maxPacketBytes int, fn func([]byte) error) error {
	if len(frame) < len(tunBatchMagic) || string(frame[:len(tunBatchMagic)]) != string(tunBatchMagic[:]) {
		if len(frame) == 0 || len(frame) > maxPacketBytes {
			return errors.New("invalid unframed tun packet")
		}
		return fn(frame)
	}
	pos := len(tunBatchMagic)
	count := 0
	for pos < len(frame) {
		if len(frame)-pos < 4 {
			return errors.New("truncated tun batch length")
		}
		length := int(binary.BigEndian.Uint32(frame[pos : pos+4]))
		pos += 4
		if length <= 0 || length > maxPacketBytes || length > len(frame)-pos {
			return fmt.Errorf("invalid tun batch packet length %d", length)
		}
		if err := fn(frame[pos : pos+length]); err != nil {
			return err
		}
		pos += length
		count++
	}
	if count == 0 {
		return errors.New("empty tun batch")
	}
	return nil
}
