package objectstorage

import "context"

type Bucket interface {
	Exists(ctx context.Context, path string) (bool, error)

	// Delete deletes the specified object. Delete will return nil if the object is not found.
	Delete(ctx context.Context, path string) error
}
