package journal

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/versions"
)

// History is the logical commit sequence reconstructed from (or destined for) a
// physical journal. It owns the only mutable record slice of the journal
// subsystem: WAL owns a History, never the reverse, and nothing outside the
// journal package reaches into the slice. Consumers read through value-returning
// accessors whose results the caller owns.
type History struct {
	base    uint64
	records []Record
}

// NewHistory starts an empty logical sequence at the given checkpoint base.
func NewHistory(base uint64) *History {
	return &History{base: base, records: []Record{}}
}

// Base is the sequence number of the checkpoint beneath this suffix; commit
// numbering starts at base+1.
func (h *History) Base() uint64 { return h.base }

// Len is the number of durable commit records held after the base.
func (h *History) Len() int { return len(h.records) }

// Head is the sequence number of the last durable commit, equal to the base
// when the suffix is empty.
func (h *History) Head() uint64 { return h.base + uint64(len(h.records)) }

// At returns a value copy of the record at suffix index i. The copy is deep
// enough that mutating it cannot alter the history.
func (h *History) At(i int) Record { return CloneRecord(h.records[i]) }

// Record returns a value copy of the record carrying sequence seq, or false
// when the sequence falls outside the retained suffix.
func (h *History) Record(seq uint64) (Record, bool) {
	if seq <= h.base || seq > h.Head() {
		return Record{}, false
	}
	return h.At(int(seq - h.base - 1)), true
}

// Page returns up to limit committed records whose sequences are greater than
// after. The returned slice and every record inside it are owned by the caller:
// the history keeps lending its canonical sequence without transferring its
// backing array or its mutability, so long histories are never copied wholesale.
func (h *History) Page(after uint64, limit int) []Record {
	if limit <= 0 || after >= h.Head() {
		return []Record{}
	}
	start := 0
	if after > h.base {
		start = int(after - h.base)
	}
	if start >= len(h.records) {
		return []Record{}
	}
	available := len(h.records) - start
	if limit > available {
		limit = available
	}
	out := make([]Record, limit)
	for i := 0; i < limit; i++ {
		out[i] = CloneRecord(h.records[start+i])
	}
	return out
}

// Append extends the logical sequence with the next contiguous record. It
// stores a defensive copy so later caller mutation cannot rewrite canonical
// history. A gap is rejected; the physical layer calls this only after the
// frame bytes have been synced.
func (h *History) Append(r Record) error {
	if r.Sequence != h.Head()+1 {
		return fmt.Errorf("noncontiguous journal sequence")
	}
	h.records = append(h.records, CloneRecord(r))
	return nil
}

// ParseHistory decodes a complete journal image (header plus framed payloads)
// into a logical History. It performs no I/O and mutates nothing: incomplete
// final frames are reported as a tail byte count for the resource layer to trim,
// while checksum, length, format and sequence failures are errors that reject
// startup exactly as the physical recovery did.
func ParseHistory(image []byte, expectedBase uint64) (*History, int64, error) {
	base, err := DecodeHeader(image)
	if err != nil {
		return nil, 0, err
	}
	if base > expectedBase {
		return nil, 0, fmt.Errorf("journal requires newer checkpoint")
	}
	frames, tail, err := scanFrames(image)
	if err != nil {
		return nil, 0, err
	}
	history := NewHistory(base)
	last := base
	for _, frame := range frames {
		payloadStart := frame.offset + 8
		payload := image[payloadStart : payloadStart+int64(frame.length)]
		r, err := decodeRecord(payload, last, frame.offset)
		if err != nil {
			return nil, 0, err
		}
		history.records = append(history.records, r)
		last = r.Sequence
	}
	return history, tail, nil
}

// CloneRecord returns a deep copy of r covering the pointer and slice fields the
// engine can populate (relation, index, write set, rows). It makes the borrowed
// logical record safe to hand to callers and to store away from the history.
func CloneRecord(r Record) Record {
	cp := r
	if r.Relation != nil {
		relation := *r.Relation
		relation.Columns = append([]catalog.Column(nil), r.Relation.Columns...)
		cp.Relation = &relation
	}
	if r.Index != nil {
		index := *r.Index
		cp.Index = &index
	}
	if r.Writes != nil {
		cp.Writes = make([]versions.Mutation, len(r.Writes))
		for i, m := range r.Writes {
			cp.Writes[i] = cloneMutation(m)
		}
	}
	return cp
}

func cloneMutation(m versions.Mutation) versions.Mutation {
	cp := m
	if m.Row != nil {
		cp.Row = datum.Row{}
		for k, v := range m.Row {
			cp.Row[k] = v
		}
	}
	return cp
}
