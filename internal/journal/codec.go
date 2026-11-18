package journal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
)

// HeaderSize is the fixed LMW1 journal header: magic, base sequence, header CRC.
const HeaderSize = 16

// MaxFrame bounds a single encoded commit frame, payload included.
const MaxFrame = 16 << 20

const magic = "LMW1"

// EncodeHeader renders the 16-byte LMW1 header for a checkpoint base sequence.
// It is pure: callers own the returned bytes and decide when to persist them.
func EncodeHeader(base uint64) []byte {
	header := make([]byte, HeaderSize)
	copy(header[:4], magic)
	binary.BigEndian.PutUint64(header[4:12], base)
	binary.BigEndian.PutUint32(header[12:], crc32.ChecksumIEEE(header[:12]))
	return header
}

// DecodeHeader validates a header image without touching any file. The returned
// base is the checkpoint sequence the journal was created against.
func DecodeHeader(image []byte) (uint64, error) {
	if len(image) < HeaderSize {
		return 0, fmt.Errorf("journal header: short image")
	}
	if string(image[:4]) != magic || crc32.ChecksumIEEE(image[:12]) != binary.BigEndian.Uint32(image[12:]) {
		return 0, fmt.Errorf("invalid journal header")
	}
	return binary.BigEndian.Uint64(image[4:12]), nil
}

// EncodeFrame renders one length+CRC+payload frame. Encoding is pure and shares
// no state with the open WAL, so recovery tests and online appends verify the
// exact same byte material.
func EncodeFrame(payload []byte) ([]byte, error) {
	if len(payload) == 0 || len(payload) > MaxFrame {
		return nil, fmt.Errorf("commit exceeds journal frame limit")
	}
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	return frame, nil
}

// frameView describes one frame discovered while scanning a journal image.
type frameView struct {
	offset int64
	length uint32
}

// scanFrames walks the framed region of an in-memory journal image. It returns
// every complete frame in order together with the byte count of any trailing
// partial frame (a short head or a short payload). A zero tail means the image
// ends exactly on a frame boundary.
func scanFrames(image []byte) (frames []frameView, tail int64, err error) {
	offset := int64(HeaderSize)
	for offset < int64(len(image)) {
		if len(image)-int(offset) < 8 {
			return frames, int64(len(image)) - offset, nil
		}
		head := image[offset : offset+8]
		length := binary.BigEndian.Uint32(head[:4])
		if length == 0 || length > MaxFrame {
			return nil, 0, fmt.Errorf("invalid journal length at %d", offset)
		}
		payloadStart := offset + 8
		payloadEnd := payloadStart + int64(length)
		if payloadEnd > int64(len(image)) {
			return frames, int64(len(image)) - offset, nil
		}
		payload := image[payloadStart:payloadEnd]
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(head[4:8]) {
			return nil, 0, fmt.Errorf("journal checksum mismatch at %d", offset)
		}
		frames = append(frames, frameView{offset: offset, length: length})
		offset = payloadEnd
	}
	return frames, 0, nil
}

// decodeRecord turns one validated payload into a commit record and checks the
// format/sequence contract against the sequence preceding it.
func decodeRecord(payload []byte, last uint64, offset int64) (Record, error) {
	var r Record
	if err := json.Unmarshal(payload, &r); err != nil {
		return Record{}, fmt.Errorf("journal json at %d: %w", offset, err)
	}
	if r.Format != 1 || r.Sequence != last+1 {
		return Record{}, fmt.Errorf("journal sequence or format mismatch at %d", offset)
	}
	return r, nil
}
