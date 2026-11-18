package engine

import "lamina/internal/journal"

type ChangePage struct {
	Records     []journal.Record `json:"records"`
	Cursor      uint64           `json:"cursor"`
	Head        uint64           `json:"head"`
	HistoryBase uint64           `json:"history_base"`
	More        bool             `json:"more"`
}

func (d *DB) Changes(after uint64, limit int) (ChangePage, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := ChangePage{Records: []journal.Record{}, Cursor: after, Head: d.sequence, HistoryBase: d.history.Base()}
	if err := d.usable(); err != nil {
		return out, err
	}
	if after < d.history.Base() {
		return out, fault("snapshot", "change history begins at %d", d.history.Base())
	}
	if after > d.sequence || limit < 1 || limit > 1000 {
		return out, fault("invalid", "invalid change cursor or limit")
	}
	// Ask for one extra record to detect a further page without the engine ever
	// holding the history's backing array. The history owns its canonical slice;
	// the page it returns is this caller's result and this caller's memory.
	page := d.history.Page(after, limit+1)
	if len(page) > limit {
		out.More = true
		page = page[:limit]
	}
	out.Records = page
	if len(page) > 0 {
		out.Cursor = page[len(page)-1].Sequence
	}
	return out, nil
}
