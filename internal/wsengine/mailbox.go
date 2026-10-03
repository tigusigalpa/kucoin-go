package wsengine

import (
	"sync"

	"github.com/tigusigalpa/kucoin-go/stream"
)

type itemKind uint8

const (
	itemFrame itemKind = iota
	itemReset
	itemGap
)

// item is one entry of a subscription queue: a data frame or an ordered
// control marker.
type item struct {
	kind  itemKind
	frame stream.Frame
	n     uint64 // generation for itemReset, dropped count for itemGap
}

type pushResult uint8

const (
	pushOK pushResult = iota
	pushDroppedOldest
	pushDroppedNewest
	pushOverflowFail
)

// mailbox is the bounded FIFO between the single socket reader (producer) and
// one subscription's delivery goroutine (consumer). The producer never blocks:
// when the frame limit is reached the OverflowPolicy decides which frame is
// sacrificed. The loss is written into the queue itself, as a gap marker at the
// exact place where frames are missing, so the consumer hears about it in order:
// after the frames that preceded the loss and before those that followed it.
// Control markers (reset, gap) are never dropped and do not count against the
// limit; a run of consecutive drops is one gap marker.
type mailbox struct {
	mu     sync.Mutex
	q      []item
	head   int
	frames int // data frames currently queued
	limit  int
	policy stream.OverflowPolicy
	closed bool
	wake   chan struct{}
}

func newMailbox(limit int, policy stream.OverflowPolicy) *mailbox {
	if limit < 1 {
		limit = 1
	}
	return &mailbox{limit: limit, policy: policy, wake: make(chan struct{}, 1)}
}

func (m *mailbox) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// pushFrame queues a data frame under the overflow policy. It never blocks.
func (m *mailbox) pushFrame(f stream.Frame) pushResult {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return pushOK
	}
	res := pushOK
	if m.frames >= m.limit {
		switch m.policy {
		case stream.DropNewest:
			m.markLossAtTailLocked()
			m.mu.Unlock()
			m.signal()
			return pushDroppedNewest
		case stream.FailSubscription:
			m.mu.Unlock()
			return pushOverflowFail
		default: // stream.DropOldest
			m.dropOldestFrameLocked()
			res = pushDroppedOldest
		}
	}
	m.q = append(m.q, item{kind: itemFrame, frame: f})
	m.frames++
	m.mu.Unlock()
	m.signal()
	return res
}

// pushReset queues an ordered connection-reset marker. It is never dropped.
func (m *mailbox) pushReset(generation uint64) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	if n := len(m.q); n > m.head && m.q[n-1].kind == itemReset {
		// Resets with no data between them say the same thing: only the latest
		// connection matters, and failed restore attempts must not pile markers up.
		m.q[n-1].n = generation
	} else {
		m.q = append(m.q, item{kind: itemReset, n: generation})
	}
	m.mu.Unlock()
	m.signal()
}

// markLossAtTailLocked records one discarded newest frame: the loss sits behind
// everything queued so far.
func (m *mailbox) markLossAtTailLocked() {
	if n := len(m.q); n > m.head && m.q[n-1].kind == itemGap {
		m.q[n-1].n++
		return
	}
	m.q = append(m.q, item{kind: itemGap, n: 1})
}

// dropOldestFrameLocked discards the oldest queued data frame and records the
// loss where the frame was: control markers stay in front of it, so a reset that
// precedes the dropped frame is still delivered first.
func (m *mailbox) dropOldestFrameLocked() {
	i := m.head
	for i < len(m.q) && m.q[i].kind != itemFrame {
		i++
	}
	if i == len(m.q) {
		return
	}
	m.frames--
	switch {
	case i > m.head && m.q[i-1].kind == itemGap:
		m.q[i-1].n++
		m.removeAtLocked(i)
	case i+1 < len(m.q) && m.q[i+1].kind == itemGap:
		m.q[i+1].n++
		m.removeAtLocked(i)
	default:
		m.q[i] = item{kind: itemGap, n: 1}
	}
}

// removeAtLocked deletes q[i]. Only control markers stand between head and the
// oldest data frame, so it moves those few one slot up instead of copying the
// frames behind i.
func (m *mailbox) removeAtLocked(i int) {
	copy(m.q[m.head+1:i+1], m.q[m.head:i])
	m.q[m.head] = item{}
	m.head++
	m.compactLocked()
}

// compactLocked reclaims the consumed prefix of the queue so a consumer that
// stalls under sustained overflow cannot make the backing array grow without
// bound.
func (m *mailbox) compactLocked() {
	switch {
	case m.head == len(m.q):
		m.q, m.head = m.q[:0], 0
	case m.head > 64 && m.head*2 > len(m.q):
		n := copy(m.q, m.q[m.head:])
		for i := n; i < len(m.q); i++ {
			m.q[i] = item{}
		}
		m.q, m.head = m.q[:n], 0
	}
}

// pop returns the next item in order, or false when the queue is empty.
func (m *mailbox) pop() (item, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.head >= len(m.q) {
		return item{}, false
	}
	it := m.q[m.head]
	m.q[m.head] = item{}
	m.head++
	if it.kind == itemFrame {
		m.frames--
	}
	m.compactLocked()
	return it, true
}

func (m *mailbox) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// closeDiscard closes the mailbox and discards anything still queued.
func (m *mailbox) closeDiscard() {
	m.mu.Lock()
	m.closed = true
	m.q, m.head, m.frames = nil, 0, 0
	m.mu.Unlock()
	m.signal()
}
