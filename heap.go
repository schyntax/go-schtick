package schtick

import "time"

type pendingEvent struct {
	ScheduledTime time.Time
	Task          *taskImpl
	RunId         int
}

func (e *pendingEvent) IsEarlierThan(a *pendingEvent) bool {
	return e.ScheduledTime.Before(a.ScheduledTime)
}

func (e *pendingEvent) Run() {
	go e.Task.runPendingEvent(e)
}

type pendingEventHeap struct {
	events []*pendingEvent
}

func (h *pendingEventHeap) Count() int {
	return len(h.events)
}

func (h *pendingEventHeap) Push(ev *pendingEvent) {
	ei := h.add(ev)
	for ei > 0 {
		pi := parentIndex(ei)
		if ev.IsEarlierThan(h.events[pi]) {
			h.swap(ei, pi)
			ei = pi
		} else {
			break
		}
	}
}

func (h *pendingEventHeap) Pop() *pendingEvent {
	if h.Count() == 0 {
		panic("Pop called on empty heap.")
	}

	ret := h.events[0]
	end := h.clearEndElement()
	count := h.Count()

	if count > 0 {
		h.events[0] = end
		ei := 0

		for {
			lci := leftChildIndex(ei)
			rci := rightChildIndex(ei)

			if lci < count && h.events[lci].IsEarlierThan(end) {
				// we know the left child is earlier than the parent, but we have to check if the right child is actually the correct parent
				if rci < count && h.events[rci].IsEarlierThan(h.events[lci]) {
					// right child is earlier than left child, so it's the correct parent
					h.swap(ei, rci)
					ei = rci
				} else {
					// left is the correct parent
					h.swap(ei, lci)
					ei = lci
				}
			} else if rci < count && h.events[rci].IsEarlierThan(end) {
				// only the right child is earlier than the parent, so we know that's the one to swap
				h.swap(ei, rci)
				ei = rci
			} else {
				break
			}
		}
	}

	return ret
}

func (h *pendingEventHeap) Peek() *pendingEvent {
	if h.Count() > 0 {
		return h.events[0]
	}

	return nil
}

func parentIndex(index int) int {
	return (index - 1) / 2
}

func leftChildIndex(index int) int {
	return 2 * (index + 1)
}

func rightChildIndex(index int) int {
	return 2*(index+1) - 1
}

func (h *pendingEventHeap) swap(a, b int) {
	temp := h.events[a]
	h.events[a] = h.events[b]
	h.events[b] = temp
}

func (h *pendingEventHeap) add(ev *pendingEvent) int {
	insertIndex := h.Count()
	h.events = append(h.events, ev)
	return insertIndex
}

func (h *pendingEventHeap) clearEndElement() *pendingEvent {
	last := h.Count() - 1
	ev := h.events[last]
	h.events = h.events[0:last]
	return ev
}
