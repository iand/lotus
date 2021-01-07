package sentinel

import (
	"context"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/lotus/chain/types"
)

var log = logging.Logger("sentinel")

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
