package journal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// JournalFileName is the single framed write-ahead log inside a data directory.
const JournalFileName = "journal.lmw"

// LockFileName is the flock file used to claim exclusive directory ownership.
const LockFileName = "lamina.lock"

// WAL is the persistent resource behind a logical History. It alone knows about
// the data directory, the exclusive flock, the open file descriptor, fsync
// outcomes and the uncertain-write poison flag. The engine never touches those
// concerns: it asks for the History and for durable appends.
type WAL struct {
	base          uint64
	file          *os.File
	lock          *os.File
	history       *History
	recoveredTail int64
	poisoned      bool
}

// Open exclusively owns dir, creates the journal when absent and reconstructs
// the logical History from the on-disk bytes. Trimming an incomplete final frame
// happens only after the exclusive directory lock is held; complete corruption
// is rejected instead of truncating.
func Open(dir string, base uint64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, LockFileName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("data directory already open: %w", err)
	}
	path := filepath.Join(dir, JournalFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		lock.Close()
		return nil, err
	}
	w := &WAL{file: f, lock: lock, base: base, history: NewHistory(base)}
	info, err := f.Stat()
	if err != nil {
		w.Close()
		return nil, err
	}
	if info.Size() == 0 {
		if err = w.writeNewHeader(base); err != nil {
			w.Close()
			return nil, err
		}
	}
	// The directory entry for a freshly created journal must be durable before
	// any commit can be acknowledged.
	directory, syncErr := os.Open(dir)
	if syncErr != nil {
		w.Close()
		return nil, syncErr
	}
	syncErr = directory.Sync()
	directory.Close()
	if syncErr != nil {
		w.Close()
		return nil, syncErr
	}
	if err = w.recover(); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) writeNewHeader(base uint64) error {
	if _, err := w.file.Write(EncodeHeader(base)); err != nil {
		return err
	}
	return w.file.Sync()
}

// recover reads the full journal image once and hands the bytes to the pure
// decoder. The resource layer contributes only what the decoder cannot own:
// reading the descriptor, truncating a short tail and repositioning the cursor.
func (w *WAL) recover() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	image, err := io.ReadAll(w.file)
	if err != nil {
		return err
	}
	history, tail, err := ParseHistory(image, w.base)
	if err != nil {
		return err
	}
	validSize := int64(len(image)) - tail
	if tail > 0 {
		// The exclusive flock from Open is already held: trimming can only run
		// as the single owner of the directory.
		if err = w.file.Truncate(validSize); err != nil {
			return err
		}
		if err = w.file.Sync(); err != nil {
			return err
		}
	}
	w.history = history
	w.base = history.Base()
	w.recoveredTail = tail
	if _, err = w.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

// History lends the engine the canonical logical commit sequence. The returned
// pointer stays owned by the WAL; callers must read through its accessors rather
// than retaining it past Close, and must not mutate records through it.
func (w *WAL) History() *History { return w.history }

// RecoveredTail reports how many bytes of an incomplete final frame were removed
// at open time, for the resource facts exposed by GET /stats.
func (w *WAL) RecoveredTail() int64 { return w.recoveredTail }

// Poisoned reports whether a write or sync left the on-disk outcome uncertain.
func (w *WAL) Poisoned() bool { return w.poisoned }

// MarkPoisoned records that a durable record cannot be safely used even though
// the bytes themselves were synced (for example the in-memory apply failed). The
// instance must stop serving until a restart reconciles the real state.
func (w *WAL) MarkPoisoned() { w.poisoned = true }

// Append encodes r, writes and fsyncs the frame, and only then extends the
// logical history. Any write/sync failure poisons the WAL because the commit's
// durability is unknowable; the sequence stays contiguous either way.
func (w *WAL) Append(r Record) error {
	if w.poisoned {
		return fmt.Errorf("journal is unavailable; restart required")
	}
	if r.Sequence != w.history.Head()+1 {
		return fmt.Errorf("noncontiguous journal sequence")
	}
	r.Format = 1
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	frame, err := EncodeFrame(payload)
	if err != nil {
		return err
	}
	if _, err = w.file.Write(frame); err != nil {
		w.poisoned = true
		return err
	}
	if err = w.file.Sync(); err != nil {
		w.poisoned = true
		return err
	}
	return w.history.Append(r)
}

// Close releases the descriptor and the exclusive directory lock.
func (w *WAL) Close() error {
	err := w.file.Close()
	w.lock.Close()
	return err
}
