package sentinel

import (
	"context"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/vm"
)

var log = logging.Logger("sentinel")

type LoggingTipSetObserver struct {
	StateManager *stmgr.StateManager
	Chain        *store.ChainStore
}

func (o *LoggingTipSetObserver) Apply(ctx context.Context, ts *types.TipSet) error {
	log.Infof("TipSetObserver.Apply(%q)", ts.Key())
	// Here we can basically perform all of visor's indexing as though we had received
	// the tipset from a watch or walk, except we have local access to the blockstore.

	// For the moment we simply log messages

	_, _, err := o.StateManager.ComputeTipSetState(ctx, ts, o)

	return err
}

func (o *LoggingTipSetObserver) Revert(ctx context.Context, ts *types.TipSet) error {
	log.Infof("TipSetObserver.Revert(%q)", ts.Key())
	return nil
}

func (o *LoggingTipSetObserver) MessageApplied(ctx context.Context, mcid cid.Cid, msg *types.Message, ret *vm.ApplyRet, implicit bool) error {
	if implicit {
		log.Infow("implicit message applied", "mcid", mcid.String())
	} else {
		log.Infow("message applied", "mcid", mcid.String())
	}
	return nil
}
