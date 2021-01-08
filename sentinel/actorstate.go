package sentinel

import (
	"context"

	// "github.com/filecoin-project/go-address"
	// "github.com/filecoin-project/go-bitfield"
	"github.com/ipfs/go-cid"
	// cbor "github.com/ipfs/go-ipld-cbor"
	// logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	// "github.com/filecoin-project/go-state-types/abi"
	// "github.com/filecoin-project/lotus/api"
	// "github.com/filecoin-project/lotus/chain/actors/adt"
	// miner "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	// "github.com/filecoin-project/lotus/chain/vm"
	// cbor "github.com/ipfs/go-ipld-cbor"
	// "github.com/filecoin-project/lotus/lib/bufbstore"
)

// Not used any more... moved it all to MinerStateExtractionContext
type ActorInfo struct {
	// Actor types.Actor
	// Address address.Address
	// ParentStateRoot cid.Cid
	// Epoch           abi.ChainEpoch
	// TipSet          types.TipSetKey
	// ParentTipSet types.TipSetKey
}

// ActorStateAPI is the minimal subset of lens.API that is needed for actor state extraction
// NOTE: this is a quick hack to get things working, but could be a lot more efficient if we rework to avoid continually
// looking up data like tipset from tipset key
// NOTE2: Turns out none of this is needed!!
type ActorStateAPI interface {
	// ChainGetTipSet(ctx context.Context, tsk types.TipSetKey) (*types.TipSet, error)
	// ChainGetBlockMessages(ctx context.Context, msg cid.Cid) (*api.BlockMessages, error)
	// StateGetReceipt(ctx context.Context, bcid cid.Cid, tsk types.TipSetKey) (*types.MessageReceipt, error)
	// // ChainHasObj(ctx context.Context, obj cid.Cid) (bool, error)
	// // ChainReadObj(ctx context.Context, obj cid.Cid) ([]byte, error)
	// // StateGetActor(ctx context.Context, addr address.Address, tsk types.TipSetKey) (*types.Actor, error)
	// StateMinerPower(ctx context.Context, addr address.Address, tsk types.TipSetKey) (*api.MinerPower, error)
	// StateReadState(ctx context.Context, addr address.Address, tsk types.TipSetKey) (*api.ActorState, error)
	// StateMinerSectors(ctx context.Context, addr address.Address, bf *bitfield.BitField, tsk types.TipSetKey) ([]*miner.SectorOnChainInfo, error)
	// Store() adt.Store
}

var _ ActorStateAPI = (*APIImpl)(nil)

type APIImpl struct {
	StateManager *stmgr.StateManager
	Chain        *store.ChainStore
}

// func (a *APIImpl) ChainGetTipSet(ctx context.Context, tsk types.TipSetKey) (*types.TipSet, error) {
// 	return a.Chain.LoadTipSet(tsk)
// }

// func (a *APIImpl) ChainGetBlockMessages(ctx context.Context, msg cid.Cid) (*api.BlockMessages, error) {
// 	b, err := a.Chain.GetBlock(msg)
// 	if err != nil {
// 		return nil, err
// 	}

// 	bmsgs, smsgs, err := a.Chain.MessagesForBlock(b)
// 	if err != nil {
// 		return nil, err
// 	}

// 	cids := make([]cid.Cid, len(bmsgs)+len(smsgs))

// 	for i, m := range bmsgs {
// 		cids[i] = m.Cid()
// 	}

// 	for i, m := range smsgs {
// 		cids[i+len(bmsgs)] = m.Cid()
// 	}

// 	return &api.BlockMessages{
// 		BlsMessages:   bmsgs,
// 		SecpkMessages: smsgs,
// 		Cids:          cids,
// 	}, nil
// }

// func (a *APIImpl) StateGetReceipt(ctx context.Context, msg cid.Cid, tsk types.TipSetKey) (*types.MessageReceipt, error) {
// 	ts, err := a.Chain.GetTipSetFromKey(tsk)
// 	if err != nil {
// 		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
// 	}
// 	return a.StateManager.GetReceipt(ctx, msg, ts)
// }

// // func (a *APIImpl) StateGetActor(ctx context.Context, addr address.Address, tsk types.TipSetKey) (*types.Actor, error) {
// // 	ts, err := a.Chain.GetTipSetFromKey(tsk)
// // 	if err != nil {
// // 		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
// // 	}
// // 	state, err := stateForTs(ctx, ts, a.Chain, a.StateManager)
// // 	if err != nil {
// // 		return nil, xerrors.Errorf("computing tipset state failed: %w", err)
// // 	}

// // 	return state.GetActor(addr)
// // }

// func stateForTs(ctx context.Context, ts *types.TipSet, cstore *store.ChainStore, smgr *stmgr.StateManager) (*state.StateTree, error) {
// 	if ts == nil {
// 		ts = cstore.GetHeaviestTipSet()
// 	}

// 	st, _, err := smgr.TipSetState(ctx, ts)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return state.LoadStateTree(cstore.Store(context.TODO()), st)
// }

// func (a *APIImpl) StateMinerPower(ctx context.Context, addr address.Address, tsk types.TipSetKey) (*api.MinerPower, error) {
// 	ts, err := a.Chain.GetTipSetFromKey(tsk)
// 	if err != nil {
// 		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
// 	}

// 	mp, net, hmp, err := stmgr.GetPower(ctx, a.StateManager, ts, addr)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &api.MinerPower{
// 		MinerPower:  mp,
// 		TotalPower:  net,
// 		HasMinPower: hmp,
// 	}, nil
// }

// func (a *APIImpl) StateReadState(ctx context.Context, addr address.Address, tsk types.TipSetKey) (*api.ActorState, error) {
// 	ts, err := a.Chain.GetTipSetFromKey(tsk)
// 	if err != nil {
// 		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
// 	}
// 	astate, err := stateForTs(ctx, ts, a.Chain, a.StateManager)
// 	if err != nil {
// 		return nil, xerrors.Errorf("computing tipset state failed: %w", err)
// 	}

// 	act, err := astate.GetActor(addr)
// 	if err != nil {
// 		return nil, xerrors.Errorf("getting actor: %w", err)
// 	}

// 	blk, err := astate.Store.(*cbor.BasicIpldStore).Blocks.Get(act.Head)
// 	if err != nil {
// 		return nil, xerrors.Errorf("getting actor head: %w", err)
// 	}

// 	oif, err := vm.DumpActorState(act, blk.RawData())
// 	if err != nil {
// 		return nil, xerrors.Errorf("dumping actor state (a:%s): %w", addr, err)
// 	}

// 	return &api.ActorState{
// 		Balance: act.Balance,
// 		State:   oif,
// 	}, nil
// }

// func (a *APIImpl) StateMinerSectors(ctx context.Context, addr address.Address, sectorNos *bitfield.BitField, tsk types.TipSetKey) ([]*miner.SectorOnChainInfo, error) {
// 	ts, err := a.Chain.GetTipSetFromKey(tsk)
// 	if err != nil {
// 		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
// 	}
// 	return stmgr.GetMinerSectorSet(ctx, a.StateManager, ts, addr, sectorNos)
// }

// func (a *APIImpl) Store() adt.Store {
// 	return a.Chain.Store(context.TODO())
// }

func (a *APIImpl) StateChangedActors(ctx context.Context, old cid.Cid, new cid.Cid) (map[string]types.Actor, error) {
	store := a.Chain.Store(ctx)

	oldTree, err := state.LoadStateTree(store, old)
	if err != nil {
		return nil, xerrors.Errorf("failed to load old state tree: %w", err)
	}

	newTree, err := state.LoadStateTree(store, new)
	if err != nil {
		return nil, xerrors.Errorf("failed to load new state tree: %w", err)
	}

	return state.Diff(oldTree, newTree)
}
