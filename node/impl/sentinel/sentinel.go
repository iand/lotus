package sentinel

import (
	// "context"

	"go.uber.org/fx"

	"github.com/filecoin-project/lotus/chain/events"
	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/chain/store"
)

type SentinelModuleAPI interface {
}

var _ SentinelModuleAPI = (*SentinelModule)(nil)

type SentinelModule struct {
	fx.In

	StateManager *stmgr.StateManager
	Chain        *store.ChainStore
	Events       *events.Events
}
