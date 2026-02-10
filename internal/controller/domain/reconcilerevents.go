package domain

type ReconcilerEvents struct {
	events []any // FIXME: should not be any
}

func NewReconcilerEvents() *ReconcilerEvents {
	return &ReconcilerEvents{
		events: []any{},
	}
}

func (e *ReconcilerEvents) Append(event any) {
	e.events = append(e.events, event)
}

func (e *ReconcilerEvents) TakeAll() []any {
	events := e.events
	e.events = []any{}

	return events
}
