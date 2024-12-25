// Code generated by MockGen. DO NOT EDIT.
// Source: internal/controller/internal/objectstorage/objectstorage.go
//
// Generated by this command:
//
//	mockgen -source=internal/controller/internal/objectstorage/objectstorage.go -destination=internal/controller/internal/objectstorage/objectstorage_mock.go -package=objectstorage
//

// Package objectstorage is a generated GoMock package.
package objectstorage

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockBucket is a mock of Bucket interface.
type MockBucket struct {
	ctrl     *gomock.Controller
	recorder *MockBucketMockRecorder
	isgomock struct{}
}

// MockBucketMockRecorder is the mock recorder for MockBucket.
type MockBucketMockRecorder struct {
	mock *MockBucket
}

// NewMockBucket creates a new mock instance.
func NewMockBucket(ctrl *gomock.Controller) *MockBucket {
	mock := &MockBucket{ctrl: ctrl}
	mock.recorder = &MockBucketMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBucket) EXPECT() *MockBucketMockRecorder {
	return m.recorder
}

// Delete mocks base method.
func (m *MockBucket) Delete(ctx context.Context, path string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", ctx, path)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockBucketMockRecorder) Delete(ctx, path any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockBucket)(nil).Delete), ctx, path)
}

// Exists mocks base method.
func (m *MockBucket) Exists(ctx context.Context, path string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Exists", ctx, path)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exists indicates an expected call of Exists.
func (mr *MockBucketMockRecorder) Exists(ctx, path any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exists", reflect.TypeOf((*MockBucket)(nil).Exists), ctx, path)
}