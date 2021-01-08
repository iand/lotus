package sentinel

import (
	"context"

	"github.com/filecoin-project/go-address"
	sa0builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	sa2builtin "github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/chain/state"
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

	// Build a lookup of all messages and the blocks they appear in
	messageBlocks := map[cid.Cid][]cid.Cid{}
	for blockIdx, bh := range ts.Blocks() {
		blscids, secpkcids, err := o.Chain.ReadMsgMetaCids(bh.Messages)
		if err != nil {
			return xerrors.Errorf("read messages for block: %w", err)
		}

		for _, c := range blscids {
			messageBlocks[c] = append(messageBlocks[c], ts.Cids()[blockIdx])
		}

		for _, c := range secpkcids {
			messageBlocks[c] = append(messageBlocks[c], ts.Cids()[blockIdx])
		}
	}

	// Replay all the messages and collect their results
	var coll messageCollector
	newStateRoot, _, err := o.StateManager.ComputeTipSetState(ctx, ts, &coll)
	if err != nil {
		return xerrors.Errorf("compute tipset state: %w", err)
	}

	log.Infow("messages collected", "count", len(coll.messages))

	ai := &APIImpl{
		StateManager: o.StateManager,
		Chain:        o.Chain,
	}

	// This could be a lot more efficient
	changes, err := ai.StateChangedActors(ctx, ts.ParentState(), newStateRoot)
	if err != nil {
		return xerrors.Errorf("getting list of changed actors: %w", err)
	}

	astore := o.Chain.Store(ctx)

	parentStateTree, err := state.LoadStateTree(astore, ts.ParentState())
	if err != nil {
		return xerrors.Errorf("load parent state tree: %w", err)
	}

	newStateTree, err := state.LoadStateTree(astore, newStateRoot)
	if err != nil {
		return xerrors.Errorf("load new state tree: %w", err)
	}

	for addrStr, act := range changes {
		log.Infow("actor change found", "addr", addrStr)

		if act.Code != sa0builtin.StorageMinerActorCodeID && act.Code != sa2builtin.StorageMinerActorCodeID {
			// Not a miner
			continue
		}

		addr, err := address.NewFromString(addrStr)
		if err != nil {
			return xerrors.Errorf("failed to parse address: %w", err)
		}

		ec, err := NewMinerStateExtractionContext(ctx, ts.Height()+1, addr, ts.ParentState(), newStateRoot, parentStateTree, newStateTree, astore)
		if err != nil {
			return xerrors.Errorf("creating miner state extraction context: %w", err)
		}

		// Not used any more... moved it all to MinerStateExtractionContext
		info := ActorInfo{
			// Actor: act,
			// Address: addr,
			// ParentStateRoot: ts.ParentState(),
			// Epoch:           ts.Height() + 1, // +1 to align it with what visor reports
			// TipSet:          pts.Key(),
			// ParentTipSet: ts,
		}

		var extractor StorageMinerExtractor

		data, err := extractor.Extract(ctx, info, ec, ai)
		if err != nil {
			return xerrors.Errorf("extract: %w", err)
		}
		log.Infow("extracted miner", "addr", addrStr)

		_ = data

	}

	return nil
}

func (o *LoggingTipSetObserver) Revert(ctx context.Context, ts *types.TipSet) error {
	log.Infof("TipSetObserver.Revert(%q)", ts.Key())
	// TODO: figure out how to handle reverts
	return nil
}

type ExecutedMessage struct {
	Cid      cid.Cid
	Message  *types.Message
	Result   *vm.ApplyRet
	Implicit bool
}

type messageCollector struct {
	messages []*ExecutedMessage
}

func (c *messageCollector) MessageApplied(ctx context.Context, mcid cid.Cid, msg *types.Message, ret *vm.ApplyRet, implicit bool) error {
	c.messages = append(c.messages, &ExecutedMessage{
		Cid:      mcid,
		Message:  msg,
		Result:   ret,
		Implicit: implicit,
	})
	return nil
}
