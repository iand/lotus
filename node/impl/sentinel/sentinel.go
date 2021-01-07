package sentinel

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/chain/events"
	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/node/impl/full"
	"github.com/filecoin-project/lotus/sentinel"
)

type SentinelAPI interface {
	// TODO: add parameter for destination of data
	// TODO: add some kind of handle so the watch can be removed
	SentinelStartWatch(ctx context.Context, confidence abi.ChainEpoch) error
}

var _ SentinelAPI = (*SentinelModule)(nil)

type SentinelModule struct {
	fx.In

	StateManager *stmgr.StateManager
	Chain        *store.ChainStore
	EventAPI     EventAPI
}

type EventAPI struct {
	*full.ChainModule
	*full.StateModule
}

// SentinelStartWatch starts a watcher that will be notified of new tipsets once the given confidence has been reached.
func (s *SentinelModule) SentinelStartWatch(ctx context.Context, confidence abi.ChainEpoch) error {
	// TODO: pass confidence
	evts := events.NewEventsWithConfidence(ctx, s.EventAPI, confidence)
	return evts.Observe(&sentinel.LoggingTipSetObserver{
		Chain:        s.Chain,
		StateManager: s.StateManager,
	})
}

var _ SentinelAPI = (*SentinelUnavailable)(nil)

// SentinelUnavailable is an implementation of the sentinel api that returns an unavailable error for every request
type SentinelUnavailable struct {
	fx.In
}

func (SentinelUnavailable) SentinelStartWatch(ctx context.Context, confidence abi.ChainEpoch) error {
	return xerrors.Errorf("sentinel unavailable")
}
