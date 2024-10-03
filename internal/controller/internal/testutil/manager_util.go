package testutil

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// ManagerUtil is a utility interface for managing a controller-runtime manager
// You can use this to start and stop the manager in a goroutine
type ManagerUtil interface {
	Start()
	Stop() error
	GetManager() manager.Manager
}

type managerUtilImpl struct {
	ctx    context.Context
	cancel context.CancelFunc
	mgr    manager.Manager
	errCh  chan error
}

func NewManagerUtil(ctxRoot context.Context, config *rest.Config, schema *runtime.Scheme) ManagerUtil {
	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme: schema,
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(ctxRoot)

	return &managerUtilImpl{
		ctx:    ctx,
		cancel: cancel,
		mgr:    mgr,
		errCh:  make(chan error),
	}
}

// Start starts the manager in a goroutine
func (m *managerUtilImpl) Start() {
	go func() {
		m.errCh <- m.mgr.Start(m.ctx)
	}()
}

// Stop stops the manager and waits for it to stop
// Returns an error if the manager failed
func (m *managerUtilImpl) Stop() error {
	m.cancel()
	return <-m.errCh
}

func (m *managerUtilImpl) GetManager() manager.Manager {
	return m.mgr
}
