// Package journal owns the durable commit history of LaminaDB and separates
// three responsibilities that the original physical WAL conflated:
//
//   - Logical history (history.go): the single contiguous commit sequence,
//     anchored at a checkpoint base. History owns the one canonical record
//     slice; it is lent to the engine through value-copying accessors, never
//     handed out as a mutable slice.
//   - Persistent resource (wal.go): the data directory, exclusive flock, file
//     descriptor, frame fsync, uncertain-write poison flag and incomplete-tail
//     trimming (performed only while the directory lock is held). A WAL owns a
//     History; the engine reaches the resource only through behavior methods.
//   - Caller results: pages and records returned by History are deep copies the
//     caller fully owns, so borrowing history never transfers lifetime or
//     mutability, while long histories are never copied wholesale.
//
// Byte decoding (codec.go) is pure and side-effect-free: the same header/frame
// routines encode online appends and decode both startup replay and recovery
// tests from identical byte material, so replay, paging, tail handling and
// reopening all verify one complete commit sequence.
package journal
