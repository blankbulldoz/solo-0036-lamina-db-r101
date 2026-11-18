package engine

import (
	"errors"
	"testing"

	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/versions"
)

// memoryLog is a durableCommitLog with no file, directory lock, fsync or WAL
// type anywhere. It pairs the engine-facing durability behavior with a real
// logical journal.History, proving the engine depends on the commit-history
// boundary rather than the physical log implementation.
type memoryLog struct {
	history *journal.History
	failAt  int
	appends int
	poison  bool
	closed  bool
	tail    int64
}

func newMemoryLog(base uint64) *memoryLog {
	return &memoryLog{history: journal.NewHistory(base)}
}

func (m *memoryLog) Append(r journal.Record) error {
	if m.poison {
		return errors.New("journal is unavailable; restart required")
	}
	m.appends++
	if m.failAt > 0 && m.appends >= m.failAt {
		m.poison = true
		return errors.New("simulated uncertain write")
	}
	if r.Sequence != m.history.Head()+1 {
		return errors.New("noncontiguous journal sequence")
	}
	m.history.Append(r)
	return nil
}

func (m *memoryLog) Poisoned() bool       { return m.poison }
func (m *memoryLog) MarkPoisoned()        { m.poison = true }
func (m *memoryLog) RecoveredTail() int64 { return m.tail }
func (m *memoryLog) Close() error         { m.closed = true; return nil }

// Compile-time assertion that the fake satisfies the engine's only persistence
// dependency.
var _ durableCommitLog = (*memoryLog)(nil)

func newEngineWithMemoryLog(t *testing.T) (*DB, *memoryLog) {
	t.Helper()
	log := newMemoryLog(0)
	d := &DB{
		catalog:      catalog.New(),
		store:        versions.New(),
		log:          log,
		history:      log.history,
		transactions: map[string]*Transaction{},
	}
	d.indexes = index.Build(d.catalog, d.store)
	return d, log
}

func TestEngineRunsAgainstInMemoryCommitBoundary(t *testing.T) {
	d, log := newEngineWithMemoryLog(t)

	schema := catalog.Relation{
		Name:    "spectra",
		Primary: "sample",
		Columns: []catalog.Column{
			{Name: "sample", Type: "int"},
			{Name: "band", Type: "text"},
			{Name: "amplitude", Type: "int", Nullable: true},
			{Name: "valid", Type: "bool"},
		},
	}
	if _, err := d.CreateRelation(schema); err != nil {
		t.Fatalf("create relation: %v", err)
	}

	tx, err := d.Begin(false)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	row := datum.Row{"sample": datum.Integer(1), "band": {Kind: "text", Text: "b1"}, "amplitude": datum.Integer(10), "valid": {Kind: "bool", Bool: true}}
	if err = d.Put(tx.ID, "spectra", row, false); err != nil {
		t.Fatalf("put: %v", err)
	}
	commit, err := d.Commit(tx.ID)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if commit.Sequence != 2 || d.sequence != 2 {
		t.Fatalf("engine sequence = %d commit=%+v", d.sequence, commit)
	}

	// The engine and the logical history must agree on one commit sequence even
	// though no physical journal object exists on this path.
	if log.history.Len() != 2 || log.history.Head() != d.sequence {
		t.Fatalf("history len=%d head=%d vs engine sequence %d", log.history.Len(), log.history.Head(), d.sequence)
	}
	r, ok := log.history.Record(2)
	if !ok || r.Kind != "commit" || len(r.Writes) != 1 {
		t.Fatalf("durable commit missing from logical history: %+v ok=%v", r, ok)
	}
}

func TestChangesPageFromLogicalHistory(t *testing.T) {
	d, _ := newEngineWithMemoryLog(t)
	schema := catalog.Relation{Name: "spectra", Primary: "sample", Columns: []catalog.Column{{Name: "sample", Type: "int"}}}
	if _, err := d.CreateRelation(schema); err != nil {
		t.Fatalf("create relation: %v", err)
	}
	for i := int64(1); i <= 4; i++ {
		tx, _ := d.Begin(false)
		_ = d.Put(tx.ID, "spectra", datum.Row{"sample": datum.Integer(i)}, false)
		if _, err := d.Commit(tx.ID); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	page, err := d.Changes(1, 2)
	if err != nil {
		t.Fatalf("changes: %v", err)
	}
	if len(page.Records) != 2 || !page.More || page.Cursor != 3 || page.Head != 5 || page.HistoryBase != 0 {
		t.Fatalf("unexpected page: %+v", page)
	}
	// The page is the caller's result: mutating it cannot reach the history.
	page.Records[0].Writes[0].Key = "tampered"
	again, err := d.Changes(1, 2)
	if err != nil {
		t.Fatalf("changes again: %v", err)
	}
	if again.Records[0].Writes[0].Key == "tampered" {
		t.Fatal("change page aliased the engine's canonical history")
	}

	if _, err = d.Changes(999, 10); err == nil {
		t.Fatal("cursor past head was accepted")
	}
}

func TestUncertainWriteStopsServiceAndConsumesNoSequence(t *testing.T) {
	d, log := newEngineWithMemoryLog(t)
	schema := catalog.Relation{Name: "spectra", Primary: "sample", Columns: []catalog.Column{{Name: "sample", Type: "int"}}}
	if _, err := d.CreateRelation(schema); err != nil {
		t.Fatalf("create relation: %v", err)
	}
	before := d.sequence

	// Fail the next durable append; the engine must surface unavailability and
	// stop serving without advancing the publish sequence.
	log.failAt = 1
	tx, _ := d.Begin(false)
	_ = d.Put(tx.ID, "spectra", datum.Row{"sample": datum.Integer(1)}, false)
	if _, err := d.Commit(tx.ID); err == nil {
		t.Fatal("uncertain write was reported as success")
	}
	if d.sequence != before {
		t.Fatalf("failed commit consumed sequence %d -> %d", before, d.sequence)
	}
	if err := d.usable(); err == nil {
		t.Fatal("engine kept serving after an uncertain journal write")
	}
	if _, err := d.Stats(); err == nil {
		t.Fatal("stats stayed available while the durable log was poisoned")
	}
	if !log.Poisoned() {
		t.Fatal("physical resource not marked uncertain")
	}
}

func TestCloseReleasesResourceOnlyOnce(t *testing.T) {
	d, log := newEngineWithMemoryLog(t)
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !log.closed {
		t.Fatal("resource Close not delegated through the boundary")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
}
