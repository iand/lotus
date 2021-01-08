package sentinel

import (
	"context"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	maddr "github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/chain/actors/adt"
	miner "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/types"
)

const (
	PreCommitAdded   = "PRECOMMIT_ADDED"
	PreCommitExpired = "PRECOMMIT_EXPIRED"

	CommitCapacityAdded = "COMMIT_CAPACITY_ADDED"

	SectorAdded      = "SECTOR_ADDED"
	SectorExtended   = "SECTOR_EXTENDED"
	SectorFaulted    = "SECTOR_FAULTED"
	SectorRecovering = "SECTOR_RECOVERING"
	SectorRecovered  = "SECTOR_RECOVERED"

	SectorExpired    = "SECTOR_EXPIRED"
	SectorTerminated = "SECTOR_TERMINATED"
)

type MinerCurrentDeadlineInfo struct {
	Height    int64  `pg:",pk,notnull,use_zero"`
	MinerID   string `pg:",pk,notnull"`
	StateRoot string `pg:",pk,notnull"`

	DeadlineIndex uint64 `pg:",notnull,use_zero"`
	PeriodStart   int64  `pg:",notnull,use_zero"`
	Open          int64  `pg:",notnull,use_zero"`
	Close         int64  `pg:",notnull,use_zero"`
	Challenge     int64  `pg:",notnull,use_zero"`
	FaultCutoff   int64  `pg:",notnull,use_zero"`
}

type MinerCurrentDeadlineInfoList []*MinerCurrentDeadlineInfo

type MinerFeeDebt struct {
	Height    int64  `pg:",pk,notnull,use_zero"`
	MinerID   string `pg:",pk,notnull"`
	StateRoot string `pg:",pk,notnull"`

	FeeDebt string `pg:",notnull"`
}

type MinerFeeDebtList []*MinerFeeDebt

type MinerLockedFund struct {
	Height    int64  `pg:",pk,notnull,use_zero"`
	MinerID   string `pg:",pk,notnull"`
	StateRoot string `pg:",pk,notnull"`

	LockedFunds       string `pg:",notnull"`
	InitialPledge     string `pg:",notnull"`
	PreCommitDeposits string `pg:",notnull"`
}

type MinerLockedFundsList []*MinerLockedFund

type MinerInfo struct {
	Height    int64  `pg:",pk,notnull,use_zero"`
	MinerID   string `pg:",pk,notnull"`
	StateRoot string `pg:",pk,notnull"`

	OwnerID  string `pg:",notnull"`
	WorkerID string `pg:",notnull"`

	NewWorker         string
	WorkerChangeEpoch int64 `pg:",notnull,use_zero"`

	ConsensusFaultedElapsed int64 `pg:",notnull,use_zero"`

	PeerID           string
	ControlAddresses []string
	MultiAddresses   []string
}

type MinerInfoList []*MinerInfo

type MinerPreCommitInfo struct {
	Height    int64  `pg:",pk,notnull,use_zero"`
	MinerID   string `pg:",pk,notnull"`
	SectorID  uint64 `pg:",pk,use_zero"`
	StateRoot string `pg:",pk,notnull"`

	SealedCID       string `pg:",notnull"`
	SealRandEpoch   int64  `pg:",use_zero"`
	ExpirationEpoch int64  `pg:",use_zero"`

	PreCommitDeposit   string `pg:",notnull"`
	PreCommitEpoch     int64  `pg:",use_zero"`
	DealWeight         string `pg:",notnull"`
	VerifiedDealWeight string `pg:",notnull"`

	IsReplaceCapacity      bool
	ReplaceSectorDeadline  uint64 `pg:",use_zero"`
	ReplaceSectorPartition uint64 `pg:",use_zero"`
	ReplaceSectorNumber    uint64 `pg:",use_zero"`
}

type MinerPreCommitInfoList []*MinerPreCommitInfo

type MinerSectorInfo struct {
	Height    int64  `pg:",pk,notnull,use_zero"`
	MinerID   string `pg:",pk,notnull"`
	SectorID  uint64 `pg:",pk,use_zero"`
	StateRoot string `pg:",pk,notnull"`

	SealedCID string `pg:",notnull"`

	ActivationEpoch int64 `pg:",use_zero"`
	ExpirationEpoch int64 `pg:",use_zero"`

	DealWeight         string `pg:",notnull"`
	VerifiedDealWeight string `pg:",notnull"`

	InitialPledge         string `pg:",notnull"`
	ExpectedDayReward     string `pg:",notnull"`
	ExpectedStoragePledge string `pg:",notnull"`
}

type MinerSectorInfoList []*MinerSectorInfo

type MinerSectorDeal struct {
	Height   int64  `pg:",pk,notnull,use_zero"`
	MinerID  string `pg:",pk,notnull"`
	SectorID uint64 `pg:",pk,use_zero"`
	DealID   uint64 `pg:",pk,use_zero"`
}

type MinerSectorDealList []*MinerSectorDeal

type MinerSectorEvent struct {
	Height    int64  `pg:",pk,notnull,use_zero"`
	MinerID   string `pg:",pk,notnull"`
	SectorID  uint64 `pg:",pk,use_zero"`
	StateRoot string `pg:",pk,notnull"`

	// https://github.com/go-pg/pg/issues/993
	// override the SQL type with enum type, see 1_chainwatch.go for enum definition
	Event string `pg:"type:miner_sector_event_type" pg:",pk,notnull"` // nolint: staticcheck
}
type MinerSectorEventList []*MinerSectorEvent

type MinerSectorPost struct {
	Height   int64  `pg:",pk,notnull,use_zero"`
	MinerID  string `pg:",pk,notnull"`
	SectorID uint64 `pg:",pk,notnull,use_zero"`

	PostMessageCID string
}

type MinerSectorPostList []*MinerSectorPost

type MinerTaskResult struct {
	Posts MinerSectorPostList

	MinerInfoModel           *MinerInfo
	FeeDebtModel             *MinerFeeDebt
	LockedFundsModel         *MinerLockedFund
	CurrentDeadlineInfoModel *MinerCurrentDeadlineInfo
	PreCommitsModel          MinerPreCommitInfoList
	SectorsModel             MinerSectorInfoList
	SectorEventsModel        MinerSectorEventList
	SectorDealsModel         MinerSectorDealList
}

type StorageMinerExtractor struct{}

func (m StorageMinerExtractor) Extract(ctx context.Context, a ActorInfo, ec *MinerStateExtractionContext, node ActorStateAPI) (*MinerTaskResult, error) {
	started := time.Now()

	minerInfoModel, err := ExtractMinerInfo(a, ec)
	if err != nil {
		return nil, xerrors.Errorf("extracting miner info: %w", err)
	}
	log.Infow("extracted miner info", "addr", ec.Address.String(), "elapsed", time.Since(started))

	lockedFundsModel, err := ExtractMinerLockedFunds(a, ec)
	if err != nil {
		return nil, xerrors.Errorf("extracting miner locked funds: %w", err)
	}
	log.Infow("extracted miner locked funds", "addr", ec.Address.String(), "elapsed", time.Since(started))

	feeDebtModel, err := ExtractMinerFeeDebt(a, ec)
	if err != nil {
		return nil, xerrors.Errorf("extracting miner fee debt: %w", err)
	}
	log.Infow("extracted miner fee debt", "addr", ec.Address.String(), "elapsed", time.Since(started))

	currDeadlineModel, err := ExtractMinerCurrentDeadlineInfo(a, ec)
	if err != nil {
		return nil, xerrors.Errorf("extracting miner current deadline info: %w", err)
	}
	log.Infow("extracted miner current deadline info", "addr", ec.Address.String(), "elapsed", time.Since(started))

	preCommitModel, sectorModel, sectorDealsModel, sectorEventsModel, err := ExtractMinerSectorData(ctx, ec, a, node)
	if err != nil {
		return nil, xerrors.Errorf("extracting miner sector changes: %w", err)
	}
	log.Infow("extracted miner sector data", "addr", ec.Address.String(), "elapsed", time.Since(started))

	// posts, err := ExtractMinerPoSts(ctx, &a, ec, node)
	// if err != nil {
	// 	return nil, xerrors.Errorf("extracting miner posts: %v", err)
	// }

	return &MinerTaskResult{
		// Posts: posts,

		MinerInfoModel:           minerInfoModel,
		LockedFundsModel:         lockedFundsModel,
		FeeDebtModel:             feeDebtModel,
		CurrentDeadlineInfoModel: currDeadlineModel,
		SectorDealsModel:         sectorDealsModel,
		SectorEventsModel:        sectorEventsModel,
		SectorsModel:             sectorModel,
		PreCommitsModel:          preCommitModel,
	}, nil
}

func NewMinerStateExtractionContext(ctx context.Context, height abi.ChainEpoch, addr address.Address, parentStateRoot, newStateRoot cid.Cid, parentStateTree, newStateTree *state.StateTree, store adt.Store) (*MinerStateExtractionContext, error) {
	curActor, err := newStateTree.GetActor(addr)
	if err != nil {
		return nil, xerrors.Errorf("loading current miner state: %w", err)
	}

	curState, err := miner.Load(store, curActor)
	if err != nil {
		return nil, xerrors.Errorf("loading previous miner actor state: %w", err)
	}

	prevState := curState
	if height != 0 {
		prevActor, err := parentStateTree.GetActor(addr)
		if err != nil {
			return nil, xerrors.Errorf("loading previous miner %s at epoch %d: %w", addr, height, err)
		}

		prevState, err = miner.Load(store, prevActor)
		if err != nil {
			return nil, xerrors.Errorf("loading previous miner actor state: %w", err)
		}
	}

	return &MinerStateExtractionContext{
		Address: addr,

		PrevState:     prevState,
		PrevStateRoot: parentStateRoot,
		CurrActor:     curActor,
		CurrState:     curState,
		CurrStateRoot: newStateRoot,
		Height:        height,
	}, nil
}

type MinerStateExtractionContext struct {
	Address       address.Address
	PrevState     miner.State
	PrevStateRoot cid.Cid

	CurrActor     *types.Actor
	CurrState     miner.State
	CurrStateRoot cid.Cid
	Height        abi.ChainEpoch
}

func (m *MinerStateExtractionContext) IsGenesis() bool {
	return m.Height == 0
}

func ExtractMinerInfo(a ActorInfo, ec *MinerStateExtractionContext) (*MinerInfo, error) {
	if ec.IsGenesis() {
		// genesis state special case.
	} else if changed, err := ec.CurrState.MinerInfoChanged(ec.PrevState); err != nil {
		return nil, err
	} else if !changed {
		return nil, nil
	}
	// miner info has changed.

	newInfo, err := ec.CurrState.Info()
	if err != nil {
		return nil, err
	}

	var newWorker string
	if newInfo.NewWorker != address.Undef {
		newWorker = newInfo.NewWorker.String()
	}

	var newCtrlAddresses []string
	for _, addr := range newInfo.ControlAddresses {
		newCtrlAddresses = append(newCtrlAddresses, addr.String())
	}

	// best effort to decode, we have no control over what miners put in this field, its just bytes.
	var newMultiAddrs []string
	for _, addr := range newInfo.Multiaddrs {
		newMaddr, err := maddr.NewMultiaddrBytes(addr)
		if err == nil {
			newMultiAddrs = append(newMultiAddrs, newMaddr.String())
		} else {
			log.Debugw("failed to decode miner multiaddr", "miner", ec.Address, "multiaddress", addr, "error", err)
		}
	}
	mi := &MinerInfo{
		Height:                  int64(ec.Height),
		MinerID:                 ec.Address.String(),
		StateRoot:               ec.PrevStateRoot.String(),
		OwnerID:                 newInfo.Owner.String(),
		WorkerID:                newInfo.Worker.String(),
		NewWorker:               newWorker,
		WorkerChangeEpoch:       int64(newInfo.WorkerChangeEpoch),
		ConsensusFaultedElapsed: int64(newInfo.ConsensusFaultElapsed),
		ControlAddresses:        newCtrlAddresses,
		MultiAddresses:          newMultiAddrs,
	}

	if newInfo.PeerId != nil {
		mi.PeerID = newInfo.PeerId.String()
	}

	return mi, nil
}

func ExtractMinerLockedFunds(a ActorInfo, ec *MinerStateExtractionContext) (*MinerLockedFund, error) {
	currLocked, err := ec.CurrState.LockedFunds()
	if err != nil {
		return nil, xerrors.Errorf("loading current miner locked funds: %w", err)
	}
	if !ec.IsGenesis() {
		prevLocked, err := ec.PrevState.LockedFunds()
		if err != nil {
			return nil, xerrors.Errorf("loading previous miner locked funds: %w", err)
		}
		if prevLocked == currLocked {
			return nil, nil
		}
	}
	// funds changed

	return &MinerLockedFund{
		Height:            int64(ec.Height),
		MinerID:           ec.Address.String(),
		StateRoot:         ec.PrevStateRoot.String(),
		LockedFunds:       currLocked.VestingFunds.String(),
		InitialPledge:     currLocked.InitialPledgeRequirement.String(),
		PreCommitDeposits: currLocked.PreCommitDeposits.String(),
	}, nil
}

func ExtractMinerFeeDebt(a ActorInfo, ec *MinerStateExtractionContext) (*MinerFeeDebt, error) {
	currDebt, err := ec.CurrState.FeeDebt()
	if err != nil {
		return nil, xerrors.Errorf("loading current miner fee debt: %w", err)
	}

	if !ec.IsGenesis() {
		prevDebt, err := ec.PrevState.FeeDebt()
		if err != nil {
			return nil, xerrors.Errorf("loading previous miner fee debt: %w", err)
		}
		if prevDebt == currDebt {
			return nil, nil
		}
	}
	// debt changed

	return &MinerFeeDebt{
		Height:    int64(ec.Height),
		MinerID:   ec.Address.String(),
		StateRoot: ec.PrevStateRoot.String(),
		FeeDebt:   currDebt.String(),
	}, nil
}

func ExtractMinerCurrentDeadlineInfo(a ActorInfo, ec *MinerStateExtractionContext) (*MinerCurrentDeadlineInfo, error) {
	currDeadlineInfo, err := ec.CurrState.DeadlineInfo(ec.Height)
	if err != nil {
		return nil, err
	}

	if !ec.IsGenesis() {
		prevDeadlineInfo, err := ec.PrevState.DeadlineInfo(ec.Height)
		if err != nil {
			return nil, err
		}
		if prevDeadlineInfo == currDeadlineInfo {
			return nil, nil
		}
	}

	return &MinerCurrentDeadlineInfo{
		Height:        int64(ec.Height),
		MinerID:       ec.Address.String(),
		StateRoot:     ec.PrevStateRoot.String(),
		DeadlineIndex: currDeadlineInfo.Index,
		PeriodStart:   int64(currDeadlineInfo.PeriodStart),
		Open:          int64(currDeadlineInfo.Open),
		Close:         int64(currDeadlineInfo.Close),
		Challenge:     int64(currDeadlineInfo.Challenge),
		FaultCutoff:   int64(currDeadlineInfo.FaultCutoff),
	}, nil
}

func ExtractMinerSectorData(ctx context.Context, ec *MinerStateExtractionContext, a ActorInfo, node ActorStateAPI) (MinerPreCommitInfoList, MinerSectorInfoList, MinerSectorDealList, MinerSectorEventList, error) {
	preCommitChanges := new(miner.PreCommitChanges)
	preCommitChanges.Added = []miner.SectorPreCommitOnChainInfo{}
	preCommitChanges.Removed = []miner.SectorPreCommitOnChainInfo{}

	sectorChanges := new(miner.SectorChanges)
	sectorChanges.Added = []miner.SectorOnChainInfo{}
	sectorChanges.Removed = []miner.SectorOnChainInfo{}
	sectorChanges.Extended = []miner.SectorExtensions{}

	sectorDealsModel := MinerSectorDealList{}
	if ec.IsGenesis() {
		msectors, err := ec.CurrState.LoadSectors(nil)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		sectorChanges.Added = make([]miner.SectorOnChainInfo, len(msectors))
		for idx, sector := range msectors {
			sectorChanges.Added[idx] = *sector
			for _, dealID := range sector.DealIDs {
				sectorDealsModel = append(sectorDealsModel, &MinerSectorDeal{
					Height:   int64(ec.Height),
					MinerID:  ec.Address.String(),
					SectorID: uint64(sector.SectorNumber),
					DealID:   uint64(dealID),
				})
			}
		}
	} else { // not genesis state, need to diff with previous state to compute changes.
		var err error
		preCommitChanges, err = miner.DiffPreCommits(ec.PrevState, ec.CurrState)
		if err != nil {
			return nil, nil, nil, nil, xerrors.Errorf("diffing miner precommits: %w", err)
		}
		log.Infow("miner precommits diff", "addr", ec.Address.String(), "added", len(preCommitChanges.Added), "removed", len(preCommitChanges.Removed))

		sectorChanges, err = miner.DiffSectors(ec.PrevState, ec.CurrState)
		if err != nil {
			return nil, nil, nil, nil, xerrors.Errorf("diffing miner sectors: %w", err)
		}

		log.Infow("miner sector data diff", "addr", ec.Address.String(), "added", len(sectorChanges.Added), "removed", len(sectorChanges.Removed), "removed", len(sectorChanges.Extended))

		for _, newSector := range sectorChanges.Added {
			for _, dealID := range newSector.DealIDs {
				sectorDealsModel = append(sectorDealsModel, &MinerSectorDeal{
					Height:   int64(ec.Height),
					MinerID:  ec.Address.String(),
					SectorID: uint64(newSector.SectorNumber),
					DealID:   uint64(dealID),
				})
			}
		}
	}
	sectorEventModel, err := extractMinerSectorEvents(ctx, node, a, ec, sectorChanges, preCommitChanges)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// transform the preCommitChanges to a model
	preCommitModel := MinerPreCommitInfoList{}
	for _, added := range preCommitChanges.Added {
		pcm := &MinerPreCommitInfo{
			Height:    int64(ec.Height),
			MinerID:   ec.Address.String(),
			SectorID:  uint64(added.Info.SectorNumber),
			StateRoot: ec.PrevStateRoot.String(),

			SealedCID:       added.Info.SealedCID.String(),
			SealRandEpoch:   int64(added.Info.SealRandEpoch),
			ExpirationEpoch: int64(added.Info.Expiration),

			PreCommitDeposit:   added.PreCommitDeposit.String(),
			PreCommitEpoch:     int64(added.PreCommitEpoch),
			DealWeight:         added.DealWeight.String(),
			VerifiedDealWeight: added.VerifiedDealWeight.String(),

			IsReplaceCapacity:      added.Info.ReplaceCapacity,
			ReplaceSectorDeadline:  added.Info.ReplaceSectorDeadline,
			ReplaceSectorPartition: added.Info.ReplaceSectorPartition,
			ReplaceSectorNumber:    uint64(added.Info.ReplaceSectorNumber),
		}
		preCommitModel = append(preCommitModel, pcm)
	}

	// transform sector changes to a model
	sectorModel := MinerSectorInfoList{}
	for _, added := range sectorChanges.Added {
		sm := &MinerSectorInfo{
			Height:                int64(ec.Height),
			MinerID:               ec.Address.String(),
			SectorID:              uint64(added.SectorNumber),
			StateRoot:             ec.PrevStateRoot.String(),
			SealedCID:             added.SealedCID.String(),
			ActivationEpoch:       int64(added.Activation),
			ExpirationEpoch:       int64(added.Expiration),
			DealWeight:            added.DealWeight.String(),
			VerifiedDealWeight:    added.VerifiedDealWeight.String(),
			InitialPledge:         added.InitialPledge.String(),
			ExpectedDayReward:     added.ExpectedDayReward.String(),
			ExpectedStoragePledge: added.ExpectedStoragePledge.String(),
		}
		sectorModel = append(sectorModel, sm)
	}
	return preCommitModel, sectorModel, sectorDealsModel, sectorEventModel, nil
}

// TODO: revisit this since we have the messages on hand already.

// func ExtractMinerPoSts(ctx context.Context, actor *ActorInfo, ec *MinerStateExtractionContext, node ActorStateAPI) (MinerSectorPostList, error) {
// 	// short circuit genesis state, no PoSt messages in genesis blocks.
// 	if ec.IsGenesis() {
// 		return nil, nil
// 	}
// 	addr := actor.Address.String()
// 	posts := make(MinerSectorPostList, 0)
// 	block := actor.TipSet.Cids()[0]
// 	height := ec.Height
// 	msgs, err := node.ChainGetBlockMessages(ctx, block) <-- parent messages
// 	if err != nil {
// 		return nil, xerrors.Errorf("diffing miner posts: %v", err)
// 	}

// 	var partitions map[uint64]miner.Partition
// 	loadPartitions := func(state miner.State, epoch abi.ChainEpoch) (map[uint64]miner.Partition, error) {
// 		info, err := state.DeadlineInfo(epoch)
// 		if err != nil {
// 			return nil, err
// 		}
// 		dline, err := state.LoadDeadline(info.Index)
// 		if err != nil {
// 			return nil, err
// 		}
// 		pmap := make(map[uint64]miner.Partition)
// 		if err := dline.ForEachPartition(func(idx uint64, p miner.Partition) error {
// 			pmap[idx] = p
// 			return nil
// 		}); err != nil {
// 			return nil, err
// 		}
// 		return pmap, nil
// 	}

// 	processPostMsg := func(msg *types.Message) error {
// 		sectors := make([]uint64, 0)
// 		rcpt, err := node.StateGetReceipt(ctx, msg.Cid(), actor.TipSet)
// 		if err != nil {
// 			return err
// 		}
// 		if rcpt == nil || rcpt.ExitCode.IsError() {
// 			return nil
// 		}
// 		params := miner.SubmitWindowedPoStParams{}
// 		if err := params.UnmarshalCBOR(bytes.NewBuffer(msg.Params)); err != nil {
// 			return err
// 		}

// 		if partitions == nil {
// 			partitions, err = loadPartitions(ec.CurrState, ec.Height)
// 			if err != nil {
// 				return err
// 			}
// 		}

// 		for _, p := range params.Partitions {
// 			all, err := partitions[p.Index].AllSectors()
// 			if err != nil {
// 				return err
// 			}
// 			proven, err := bitfield.SubtractBitField(all, p.Skipped)
// 			if err != nil {
// 				return err
// 			}

// 			if err := proven.ForEach(func(sector uint64) error {
// 				sectors = append(sectors, sector)
// 				return nil
// 			}); err != nil {
// 				return err
// 			}
// 		}

// 		for _, s := range sectors {
// 			posts = append(posts, &MinerSectorPost{
// 				Height:         int64(height),
// 				MinerID:        addr,
// 				SectorID:       s,
// 				PostMessageCID: msg.Cid().String(),
// 			})
// 		}
// 		return nil
// 	}

// 	for _, msg := range msgs.BlsMessages {
// 		if msg.To == actor.Address && msg.Method == 5 /* miner.SubmitWindowedPoSt */ {
// 			if err := processPostMsg(msg); err != nil {
// 				return nil, err
// 			}
// 		}
// 	}
// 	for _, msg := range msgs.SecpkMessages {
// 		if msg.Message.To == actor.Address && msg.Message.Method == 5 /* miner.SubmitWindowedPoSt */ {
// 			if err := processPostMsg(&msg.Message); err != nil {
// 				return nil, err
// 			}
// 		}
// 	}
// 	return posts, nil
// }

func extractMinerSectorEvents(ctx context.Context, node ActorStateAPI, a ActorInfo, ec *MinerStateExtractionContext, sc *miner.SectorChanges, pc *miner.PreCommitChanges) (MinerSectorEventList, error) {
	ps, err := extractMinerPartitionsDiff(ctx, ec)
	if err != nil {
		return nil, xerrors.Errorf("extracting miner partition diff: %w", err)
	}

	out := MinerSectorEventList{}
	sectorAdds := make(map[abi.SectorNumber]miner.SectorOnChainInfo)

	// if there were changes made to the miners partition lists
	if ps != nil {
		// build an index of removed sector expiration's for comparison below.
		removedSectors, err := ec.CurrState.LoadSectors(&ps.Removed)
		if err != nil {
			return nil, xerrors.Errorf("fetching miners removed sectors: %w", err)
		}

		log.Infow("miner removed sectors", "count", len(removedSectors))

		rmExpireIndex := make(map[uint64]abi.ChainEpoch)
		for _, rm := range removedSectors {
			rmExpireIndex[uint64(rm.SectorNumber)] = rm.Expiration
		}

		// track terminated and expired sectors
		if err := ps.Removed.ForEach(func(u uint64) error {
			event := SectorTerminated
			expiration := rmExpireIndex[u]
			if expiration == ec.Height {
				event = SectorExpired
			}
			out = append(out, &MinerSectorEvent{
				Height:    int64(ec.Height),
				MinerID:   ec.Address.String(),
				StateRoot: ec.PrevStateRoot.String(),
				SectorID:  u,
				Event:     event,
			})
			return nil
		}); err != nil {
			return nil, xerrors.Errorf("walking miners removed sectors: %w", err)
		}

		// track recovering sectors
		if err := ps.Recovering.ForEach(func(u uint64) error {
			out = append(out, &MinerSectorEvent{
				Height:    int64(ec.Height),
				MinerID:   ec.Address.String(),
				StateRoot: ec.PrevStateRoot.String(),
				SectorID:  u,
				Event:     SectorRecovering,
			})
			return nil
		}); err != nil {
			return nil, xerrors.Errorf("walking miners recovering sectors: %w", err)
		}

		// track faulted sectors
		if err := ps.Faulted.ForEach(func(u uint64) error {
			out = append(out, &MinerSectorEvent{
				Height:    int64(ec.Height),
				MinerID:   ec.Address.String(),
				StateRoot: ec.PrevStateRoot.String(),
				SectorID:  u,
				Event:     SectorFaulted,
			})
			return nil
		}); err != nil {
			return nil, xerrors.Errorf("walking miners faulted sectors: %w", err)
		}

		// track recovered sectors
		if err := ps.Recovered.ForEach(func(u uint64) error {
			out = append(out, &MinerSectorEvent{
				Height:    int64(ec.Height),
				MinerID:   ec.Address.String(),
				StateRoot: ec.PrevStateRoot.String(),
				SectorID:  u,
				Event:     SectorRecovered,
			})
			return nil
		}); err != nil {
			return nil, xerrors.Errorf("walking miners recovered sectors: %w", err)
		}
	}

	// if there were changes made to the miners sectors list
	if sc != nil {
		// track sector add and commit-capacity add
		for _, add := range sc.Added {
			event := SectorAdded
			if len(add.DealIDs) == 0 {
				event = CommitCapacityAdded
			}
			out = append(out, &MinerSectorEvent{
				Height:    int64(ec.Height),
				MinerID:   ec.Address.String(),
				StateRoot: ec.PrevStateRoot.String(),
				SectorID:  uint64(add.SectorNumber),
				Event:     event,
			})
			sectorAdds[add.SectorNumber] = add
		}

		// track sector extensions
		for _, mod := range sc.Extended {
			out = append(out, &MinerSectorEvent{
				Height:    int64(ec.Height),
				MinerID:   ec.Address.String(),
				StateRoot: ec.PrevStateRoot.String(),
				SectorID:  uint64(mod.To.SectorNumber),
				Event:     SectorExtended,
			})
		}

	}

	// if there were changes made to the miners precommit list
	if pc != nil {
		// track precommit addition
		for _, add := range pc.Added {
			out = append(out, &MinerSectorEvent{
				Height:    int64(ec.Height),
				MinerID:   ec.Address.String(),
				StateRoot: ec.PrevStateRoot.String(),
				SectorID:  uint64(add.Info.SectorNumber),
				Event:     PreCommitAdded,
			})
		}
	}

	return out, nil
}

// PartitionStatus contains bitfileds of sectorID's that are removed, faulted, recovered and recovering.
type PartitionStatus struct {
	Removed    bitfield.BitField
	Faulted    bitfield.BitField
	Recovering bitfield.BitField
	Recovered  bitfield.BitField
}

func extractMinerPartitionsDiff(ctx context.Context, ec *MinerStateExtractionContext) (*PartitionStatus, error) {
	// short circuit genesis state.
	if ec.IsGenesis() {
		return nil, nil
	}

	dlDiff, err := miner.DiffDeadlines(ec.PrevState, ec.CurrState)
	if err != nil {
		return nil, err
	}

	if dlDiff == nil {
		return nil, nil
	}

	log.Infow("miner deadlines diff", "count", len(dlDiff))

	removed := bitfield.New()
	faulted := bitfield.New()
	recovered := bitfield.New()
	recovering := bitfield.New()

	for _, deadline := range dlDiff {
		for _, partition := range deadline {
			removed, err = bitfield.MergeBitFields(removed, partition.Removed)
			if err != nil {
				return nil, err
			}
			faulted, err = bitfield.MergeBitFields(faulted, partition.Faulted)
			if err != nil {
				return nil, err
			}
			recovered, err = bitfield.MergeBitFields(recovered, partition.Recovered)
			if err != nil {
				return nil, err
			}
			recovering, err = bitfield.MergeBitFields(recovering, partition.Recovering)
			if err != nil {
				return nil, err
			}
		}
	}
	return &PartitionStatus{
		Removed:    removed,
		Faulted:    faulted,
		Recovering: recovering,
		Recovered:  recovered,
	}, nil
}
