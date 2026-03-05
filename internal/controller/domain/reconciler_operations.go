package domain

// Operation represents a single reconciler operation to be executed.
type Operation any

// ReconcilerOperations is a collection of operations produced by a reconciler.
type ReconcilerOperations struct {
	operations []Operation
}

// NewReconcilerOperations creates a new empty ReconcilerOperations instance.
func NewReconcilerOperations() *ReconcilerOperations {
	return &ReconcilerOperations{
		operations: []Operation{},
	}
}

// Append adds an operation to the collection.
func (e *ReconcilerOperations) Append(operation Operation) {
	e.operations = append(e.operations, operation)
}

// TakeAll returns all operations and clears the internal collection.
func (e *ReconcilerOperations) TakeAll() []Operation {
	operations := e.operations
	e.operations = []Operation{}

	return operations
}
