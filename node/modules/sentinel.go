package modules

import (
	"context"

	"go.uber.org/fx"

	"github.com/filecoin-project/lotus/chain/events"
	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/node/impl/full"
	"github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/filecoin-project/lotus/sentinel"
)

func SentinelObserver(mctx helpers.MetricsCtx, lc fx.Lifecycle, sm *stmgr.StateManager, chainAPI full.ChainModuleAPI, stateAPI full.StateModuleAPI) {
	api := struct {
		full.ChainModuleAPI
		full.StateModuleAPI
	}{
		ChainModuleAPI: chainAPI,
		StateModuleAPI: stateAPI,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Infof("Starting SentinelObserver")
			evts := events.NewEventsWithConfidence(mctx, api, 10)
			// Add a logging observer for now but this is where we would add the sentinel indexer, with access to the API it needs
			evts.Observe(&sentinel.LoggingTipSetObserver{})
			return nil
		},
		OnStop: func(context.Context) error {
			log.Infof("Stopping SentinelObserver")
			return nil
		},
	})
}
