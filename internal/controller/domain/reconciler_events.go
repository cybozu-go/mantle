package domain

type Event any
type ReconcilerEvents struct {
	events []Event // FIXME: should not be any
}

func NewReconcilerEvents() *ReconcilerEvents {
	return &ReconcilerEvents{
		events: []Event{},
	}
}

func (e *ReconcilerEvents) Append(event Event) {
	e.events = append(e.events, event)
}

func (e *ReconcilerEvents) TakeAll() []Event {
	events := e.events
	e.events = []Event{}

	return events
}
