package store

import (
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/transaction"
)

// TxKind is the tx.kind of a transaction.
func TxKind(tx *transaction.Transaction) int {
	switch {
	case tx.IsBranchTransaction():
		return KindBranch
	case tx.IsSequencerTransaction():
		return KindSequencer
	case tx.IsMiningTransaction():
		return KindMining
	}
	return KindPlain
}

// SequencerID is the chain ID of a sequencer transaction's sequencer output.
func SequencerID(tx *transaction.Transaction) (ret base.ChainID, ok bool) {
	if !tx.IsSequencerTransaction() {
		return
	}
	tx.ForEachProducedOutput(func(_ byte, o *ledger.Output, oid base.OutputID) bool {
		if !o.IsSequencerOutput() {
			return true
		}
		if cc := o.ChainConstraint(); cc != nil {
			ret, ok = cc.ChainID, true
			if cc.IsOrigin() {
				ret = base.MakeOriginChainID(oid)
			}
		}
		return false
	})
	return
}

// Stem is the stem lock of a branch transaction, nil for any other.
func Stem(tx *transaction.Transaction) (stem *ledger.StemLock) {
	if !tx.IsBranchTransaction() {
		return nil
	}
	tx.ForEachProducedOutput(func(_ byte, o *ledger.Output, _ base.OutputID) bool {
		if s, ok := o.StemLock(); ok {
			stem = s
			return false
		}
		return true
	})
	return
}

// OutputFacts are the indexed columns of an output row.
type OutputFacts struct {
	Lock      string // "" if the lock is not recognised
	Account   []byte // the first non-empty index value, nil if none
	ChainID   *base.ChainID
	ChainKind int
}

func OutputFactsOf(o *ledger.Output, oid base.OutputID) (ret OutputFacts) {
	if lock, err := ledger.LockFromOutputElements(o.MustAt(int(ledger.ConstraintIndexIndexValues)), o.MustAt(int(ledger.ConstraintIndexLock))); err == nil {
		ret.Lock = lock.Name()
	}
	for _, v := range o.IndexValues() {
		if len(v) > 0 {
			ret.Account = v
			break
		}
	}
	if cc := o.ChainConstraint(); cc != nil {
		id := cc.ChainID
		if cc.IsOrigin() {
			id = base.MakeOriginChainID(oid)
		}
		ret.ChainID = &id
		switch {
		case id == base.MineChainID:
			ret.ChainKind = ChainMine
		case o.IsSequencerOutput():
			ret.ChainKind = ChainSequencer
		case o.DelegationLock() != nil:
			ret.ChainKind = ChainDelegation
		default:
			ret.ChainKind = ChainOther
		}
	}
	return
}
