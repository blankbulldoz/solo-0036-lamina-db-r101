package journal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/versions"
)

func commitRecord(seq uint64, keys ...string) Record {
	writes := make([]versions.Mutation, 0, len(keys))
	for _, key := range keys {
		writes = append(writes, versions.Mutation{
			Relation: "spectra",
			Key:      key,
			Row:      datum.Row{"sample": datum.Integer(int64(seq))},
		})
	}
	return Record{Format: 1, Sequence: seq, Kind: "commit", Writes: writes}
}

func relationRecord(seq uint64, name string) Record {
	return Record{
		Format:   1,
		Sequence: seq,
		Kind:     "relation",
		Relation: &catalog.Relation{
			Name:    name,
			Primary: "sample",
			Columns: []catalog.Column{{Name: "sample", Type: "int"}},
			Created: seq,
		},
	}
}

// buildImage renders a complete journal file image for records starting after a
// checkpoint base. Every consumer under test (pure parser, on-disk WAL, tail
// corruption matrix) receives these exact same bytes.
func buildImage(t *testing.T, base uint64, records ...Record) []byte {
	t.Helper()
	image := EncodeHeader(base)
	last := base
	for _, r := range records {
		if r.Sequence != last+1 {
			t.Fatalf("test setup: noncontiguous sequence %d after %d", r.Sequence, last)
		}
		r.Format = 1
		payload, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshal record: %v", err)
		}
		frame, err := EncodeFrame(payload)
		if err != nil {
			t.Fatalf("encode frame: %v", err)
		}
		image = append(image, frame...)
		last = r.Sequence
	}
	return image
}

func writeJournal(t *testing.T, dir string, image []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, JournalFileName), image, 0o600); err != nil {
		t.Fatalf("write journal: %v", err)
	}
}

func TestParseHistoryReplaysSameBytesAsWAL(t *testing.T) {
	records := []Record{
		relationRecord(1, "spectra"),
		commitRecord(2, "int:1", "int:2"),
		commitRecord(3, "int:3"),
	}
	image := buildImage(t, 0, records...)

	// Path A: the side-effect-free decoder over raw bytes.
	history, tail, err := ParseHistory(image, 0)
	if err != nil {
		t.Fatalf("ParseHistory: %v", err)
	}
	if tail != 0 {
		t.Fatalf("clean image reported tail %d", tail)
	}
	if history.Base() != 0 || history.Len() != 3 || history.Head() != 3 {
		t.Fatalf("unexpected history shape base=%d len=%d head=%d", history.Base(), history.Len(), history.Head())
	}
	for i, want := range records {
		if got := history.At(i); got.Sequence != want.Sequence || got.Kind != want.Kind {
			t.Fatalf("record %d mismatch: got %+v want %+v", i, got, want)
		}
	}
	// Paging reads the same decoded sequence without ever materializing it all:
	// one record per page, cursor walking from the base to the head.
	paged := history.Page(0, 1)
	if len(paged) != 1 || paged[0].Sequence != 1 {
		t.Fatalf("first page wrong: %+v", paged)
	}
	for cursor := uint64(1); cursor < history.Head(); cursor++ {
		next := history.Page(cursor, 1)
		if len(next) != 1 || next[0].Sequence != cursor+1 {
			t.Fatalf("page after cursor %d broke the contiguous sequence: %+v", cursor, next)
		}
	}
	if len(history.Page(history.Head(), 1)) != 0 {
		t.Fatal("page at head must be empty")
	}

	// Path B: the physical WAL over an identical file must reconstruct the same
	// logical sequence from the same byte material.
	dir := t.TempDir()
	writeJournal(t, dir, image)
	w, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	disk := w.History()
	if disk.Base() != history.Base() || disk.Len() != history.Len() || disk.Head() != history.Head() {
		t.Fatalf("disk history %d/%d/%d != parsed %d/%d/%d", disk.Base(), disk.Len(), disk.Head(), history.Base(), history.Len(), history.Head())
	}
	for i := range records {
		got, want := disk.At(i), history.At(i)
		if got.Kind != want.Kind || got.Sequence != want.Sequence || len(got.Writes) != len(want.Writes) {
			t.Fatalf("disk record %d diverges from parsed record", i)
		}
	}
	if w.RecoveredTail() != 0 {
		t.Fatalf("clean journal reported recovered tail %d", w.RecoveredTail())
	}
}

func TestPaginationOwnsCopiedSlices(t *testing.T) {
	records := []Record{
		commitRecord(1, "int:1"),
		commitRecord(2, "int:2"),
		commitRecord(3, "int:3"),
		commitRecord(4, "int:4"),
	}
	history, _, err := ParseHistory(buildImage(t, 0, records...), 0)
	if err != nil {
		t.Fatalf("ParseHistory: %v", err)
	}

	page := history.Page(1, 2)
	if len(page) != 2 || page[0].Sequence != 2 || page[1].Sequence != 3 {
		t.Fatalf("unexpected page: %+v", page)
	}
	// The page is the caller's memory: mutating it must not rewrite the history.
	page[0].Kind = "tampered"
	page[0].Writes[0].Key = "int:999"
	again, ok := history.Record(2)
	if !ok || again.Kind != "commit" || again.Writes[0].Key != "int:2" {
		t.Fatalf("history aliased a paged record: %+v ok=%v", again, ok)
	}

	// At() returns copies too.
	first := history.At(0)
	first.Relation = &catalog.Relation{Name: "x"}
	if canonical, _ := history.Record(1); canonical.Relation != nil {
		t.Fatalf("At() aliased relation pointer into history")
	}

	// Page never copies the whole history; an over-long limit returns the tail.
	tail := history.Page(3, 100)
	if len(tail) != 1 || tail[0].Sequence != 4 {
		t.Fatalf("tail page wrong: %+v", tail)
	}
	if len(history.Page(4, 10)) != 0 {
		t.Fatalf("page at head must be empty")
	}
}

func TestIncompleteTailIsTrimmedOnlyUnderLock(t *testing.T) {
	records := []Record{commitRecord(1, "int:1"), commitRecord(2, "int:2")}
	image := buildImage(t, 0, records...)
	cases := map[string]struct {
		damaged      []byte
		tail         int64
		wantRecords  int
		trimmedBytes int
	}{
		// Only three bytes of the first frame survive: everything after the
		// header is the trimmable tail.
		"short head": {image[:HeaderSize+3], 3, 0, HeaderSize},
		// The complete image plus three dangling bytes: only the dangling short
		// frame is removed, both complete commits survive.
		"short frame": {append(append([]byte(nil), image...), 0, 0, 0), 3, 2, len(image)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeJournal(t, dir, tc.damaged)
			w, err := Open(dir, 0)
			if err != nil {
				t.Fatalf("Open rejected truncatable tail: %v", err)
			}
			if w.RecoveredTail() != tc.tail {
				t.Fatalf("tail bytes = %d, want %d", w.RecoveredTail(), tc.tail)
			}
			history := w.History()
			if history.Len() != tc.wantRecords || int(history.Head()) != tc.wantRecords {
				t.Fatalf("trimmed history wrong: len=%d head=%d", history.Len(), history.Head())
			}
			if err = w.Append(commitRecord(uint64(tc.wantRecords+1), "int:next")); err != nil {
				t.Fatalf("append after trim: %v", err)
			}
			if err = w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			// Reopening the trimmed, extended directory must validate the same
			// bytes again through the pure decoder with zero remaining tail.
			reopened, err := Open(dir, 0)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()
			if reopened.RecoveredTail() != 0 || int(reopened.History().Head()) != tc.wantRecords+1 {
				t.Fatalf("reopen state tail=%d head=%d", reopened.RecoveredTail(), reopened.History().Head())
			}
			onDisk, err := os.ReadFile(filepath.Join(dir, JournalFileName))
			if err != nil {
				t.Fatalf("read back: %v", err)
			}
			if len(onDisk) <= tc.trimmedBytes {
				t.Fatalf("appended frame missing: file %d bytes, trimmed boundary %d", len(onDisk), tc.trimmedBytes)
			}
			if string(onDisk[:tc.trimmedBytes]) != string(tc.damaged[:tc.trimmedBytes]) {
				t.Fatalf("trimming altered bytes before the incomplete tail")
			}
		})
	}
}

func TestCompleteCorruptionIsRejectedNotTruncated(t *testing.T) {
	image := buildImage(t, 0, commitRecord(1, "int:1"), commitRecord(2, "int:2"))
	dir := t.TempDir()
	writeJournal(t, dir, image)

	// Flip the final payload byte: the frame is complete but its CRC fails.
	broken := append([]byte(nil), image...)
	broken[len(broken)-1] ^= 0xFF
	writeJournal(t, dir, broken)

	if _, err := Open(dir, 0); err == nil {
		t.Fatal("Open accepted a checksum-corrupt complete frame")
	}
	// The corrupt bytes must remain on disk; recovery fails closed.
	onDisk, err := os.ReadFile(filepath.Join(dir, JournalFileName))
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(onDisk) != string(broken) {
		t.Fatal("corrupt journal was modified instead of rejected")
	}
}

func TestSequenceAndHeaderFailuresRejectOpen(t *testing.T) {
	good := buildImage(t, 0, commitRecord(1, "int:1"))

	t.Run("gap in sequence", func(t *testing.T) {
		// Hand-build a journal whose second frame carries sequence 3 after 1:
		// the logical parser must reject the gap rather than silently healing it.
		gap := EncodeHeader(0)
		for _, r := range []Record{commitRecord(1, "int:1"), commitRecord(3, "int:3")} {
			r.Format = 1
			payload, mErr := json.Marshal(r)
			if mErr != nil {
				t.Fatalf("marshal: %v", mErr)
			}
			frame, fErr := EncodeFrame(payload)
			if fErr != nil {
				t.Fatalf("frame: %v", fErr)
			}
			gap = append(gap, frame...)
		}
		dir := t.TempDir()
		writeJournal(t, dir, gap)
		if _, err := Open(dir, 0); err == nil {
			t.Fatal("accepted noncontiguous journal")
		}
	})
	t.Run("journal needs newer checkpoint", func(t *testing.T) {
		// The image was written against base 5; opening against an older
		// checkpoint (base 4) means the journal requires a newer checkpoint.
		newer := buildImage(t, 5, commitRecord(6, "int:6"))
		dir := t.TempDir()
		writeJournal(t, dir, newer)
		if _, err := Open(dir, 4); err == nil {
			t.Fatal("accepted journal ahead of its checkpoint base")
		}
		// The matching expected base opens normally and anchors the history at 5.
		okDir := t.TempDir()
		writeJournal(t, okDir, newer)
		w, err := Open(okDir, 5)
		if err != nil {
			t.Fatalf("matching base refused: %v", err)
		}
		defer w.Close()
		if w.History().Base() != 5 || w.History().Head() != 6 {
			t.Fatalf("history not anchored at journal base")
		}
	})
	t.Run("bad header", func(t *testing.T) {
		dir := t.TempDir()
		writeJournal(t, dir, good[:HeaderSize-1])
		if _, err := Open(dir, 0); err == nil {
			t.Fatal("accepted truncated header")
		}
	})
}

func TestExclusiveDirectoryAndAppendPoison(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer first.Close()

	if _, err := Open(dir, 0); err == nil {
		t.Fatal("second Open on the same directory was not rejected")
	}

	if err := first.Append(commitRecord(1, "int:1")); err != nil {
		t.Fatal("initial append failed")
	}
	first.MarkPoisoned()
	if !first.Poisoned() {
		t.Fatal("poison flag not observable")
	}
	if err := first.Append(commitRecord(2, "int:2")); err == nil {
		t.Fatal("append after uncertain write was accepted")
	}
}

func TestAppendIsContiguousAndDurable(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 7)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	if w.History().Base() != 7 || w.History().Head() != 7 {
		t.Fatalf("fresh history not anchored at base 7")
	}
	if err := w.Append(commitRecord(9, "int:9")); err == nil {
		t.Fatal("accepted a sequence gap")
	}
	if err := w.Append(commitRecord(8, "int:8")); err != nil {
		t.Fatalf("contiguous append: %v", err)
	}
	if err := w.Append(commitRecord(9, "int:9")); err != nil {
		t.Fatalf("second append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(dir, 7)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	h := reopened.History()
	if h.Base() != 7 || h.Len() != 2 || h.Head() != 9 {
		t.Fatalf("durable history wrong: base=%d len=%d head=%d", h.Base(), h.Len(), h.Head())
	}
	if r, ok := h.Record(8); !ok || r.Writes[0].Key != "int:8" {
		t.Fatalf("record lookup after reopen failed: %+v ok=%v", r, ok)
	}
}

func TestPagingRespectsCheckpointBase(t *testing.T) {
	// A suffix journal sitting on checkpoint base 5 holds sequences 6..8.
	image := buildImage(t, 5, commitRecord(6, "int:6"), commitRecord(7, "int:7"), commitRecord(8, "int:8"))
	history, _, err := ParseHistory(image, 5)
	if err != nil {
		t.Fatalf("ParseHistory: %v", err)
	}
	if history.Base() != 5 || history.Head() != 8 {
		t.Fatalf("base/head wrong: %d/%d", history.Base(), history.Head())
	}
	if _, ok := history.Record(5); ok {
		t.Fatal("base sequence itself must not be a retrievable record")
	}
	// A cursor at the base returns the full suffix; cursors before the base are a
	// caller-side 410, and the engine boundary still gets an empty-but-valid page.
	if page := history.Page(5, 10); len(page) != 3 || page[0].Sequence != 6 {
		t.Fatalf("base-anchored page wrong: %+v", page)
	}
	if page := history.Page(7, 1); len(page) != 1 || page[0].Sequence != 8 {
		t.Fatalf("mid-suffix page wrong: %+v", page)
	}
}

func TestHistoryDoesNotAliasAppendedRecord(t *testing.T) {
	h := NewHistory(0)
	r := commitRecord(1, "int:1")
	if err := h.Append(r); err != nil {
		t.Fatalf("append: %v", err)
	}
	r.Writes[0].Key = "int:changed"
	if got, _ := h.Record(1); got.Writes[0].Key != "int:1" {
		t.Fatalf("history retained a mutable reference to the appended record")
	}
	if err := h.Append(commitRecord(3, "int:3")); err == nil {
		t.Fatal("logical history accepted a sequence gap")
	}
	if h.Len() != 1 || h.Head() != 1 {
		t.Fatalf("rejected append mutated history: len=%d head=%d", h.Len(), h.Head())
	}
}
