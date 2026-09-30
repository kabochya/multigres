// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

func (pm *MultipoolerManager) servingOperationChange() <-chan struct{} {
	pm.operationMu.Lock()
	defer pm.operationMu.Unlock()
	if pm.operationChanged == nil {
		pm.operationChanged = make(chan struct{})
	}
	return pm.operationChanged
}

func (pm *MultipoolerManager) notifyServingOperation() {
	pm.operationMu.Lock()
	defer pm.operationMu.Unlock()
	if pm.operationChanged != nil {
		close(pm.operationChanged)
	}
	pm.operationChanged = make(chan struct{})
}

func (pm *MultipoolerManager) servingShutdown() <-chan struct{} {
	if pm.shutdownCtx != nil {
		return pm.shutdownCtx.Done()
	}
	return nil
}

func (pm *MultipoolerManager) servingRunDone() <-chan struct{} {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.ctx != nil {
		return pm.ctx.Done()
	}
	return nil
}
