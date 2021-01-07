package modules

import (
	"context"

	"go.uber.org/fx"

	"github.com/filecoin-project/lotus/chain/events"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/impl/full"
	"github.com/filecoin-project/lotus/node/modules/helpers"
)

func SentinelObserver(mctx helpers.MetricsCtx, lc fx.Lifecycle, chainAPI full.ChainModuleAPI, stateAPI full.StateModuleAPI) {
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
			evts.Observe(&LoggingTipSetObserver{})
			return nil
		},
		OnStop: func(context.Context) error {
			log.Infof("Stopping SentinelObserver")
			return nil
		},
	})
}

type LoggingTipSetObserver struct {
}

func (o *LoggingTipSetObserver) Apply(ctx context.Context, ts *types.TipSet) error {
	log.Infof("TipSetObserver.Apply(%q)", ts.Key())
	// Here we can basically perform all of visor's indexing as though we had received
	// the tipset from a watch or walk, except we have local access to the blockstore.
	return nil
}

func (o *LoggingTipSetObserver) Revert(ctx context.Context, ts *types.TipSet) error {
	log.Infof("TipSetObserver.Revert(%q)", ts.Key())
	return nil
}
