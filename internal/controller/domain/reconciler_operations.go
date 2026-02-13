package domain

type Operation any
type ReconcilerOperations struct {
	operations []Operation
}

func NewReconcilerOperations() *ReconcilerOperations {
	return &ReconcilerOperations{
		operations: []Operation{},
	}
}

func (e *ReconcilerOperations) Append(operation Operation) {
	e.operations = append(e.operations, operation)
}

func (e *ReconcilerOperations) TakeAll() []Operation {
	operations := e.operations
	e.operations = []Operation{}

	return operations
}
