package api

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger"
)

// Libraries are the ledger libraries a dagstore runs with (SPEC.md section 7.1): those stored in
// its database, each proven, fetched with its proof or read from a node's own state, and those this
// build lists that the database does not hold yet, used from the build at their slot until the
// network's are fetched and compared. The ledger's cache cannot take a library while running, so
// one the build does not list is used only after a restart.
type Libraries struct {
	mu         sync.Mutex
	stored     []store.Library
	built      map[uint32][]byte
	builtHash  map[uint32][]byte
	alarms     []string
	alarmIndex map[string]bool
}

// LoadLedger loads into the ledger's library cache the stored libraries and, for every upgrade this
// build lists that is not stored, the build's library.
func LoadLedger(db *sql.DB) (*Libraries, error) {
	stored, err := store.LoadLibraries(db)
	if err != nil {
		return nil, err
	}
	built, err := ledger.BuildUpgradeLibraries(stored[0].JSON)
	if err != nil {
		return nil, err
	}
	l := &Libraries{stored: stored, built: built, builtHash: make(map[uint32][]byte), alarmIndex: make(map[string]bool)}
	m := make(map[uint32][]byte, len(built))
	for slot, js := range built {
		if l.builtHash[slot], err = libraryHash(js); err != nil {
			return nil, fmt.Errorf("library of slot %d of this build: %w", slot, err)
		}
		m[slot] = js
	}
	for _, s := range stored {
		m[s.ActivationSlot] = s.JSON // the proven library wins over the build's; a difference is an alarm
	}
	ledger.MustInitLibraryCacheFromMap(m)
	if h := ledger.L(0).LibraryHash(); !bytes.Equal(h[:], stored[0].Hash) {
		return nil, errors.New("the stored genesis library does not hash to its stored hash")
	}
	for _, info := range l.Info() {
		if info.Alarm {
			l.Alarm(fmt.Sprintf("library of slot %d: %s", info.ActivationSlot, info.Check))
		}
	}
	return l, nil
}

func libraryHash(js []byte) ([]byte, error) {
	lib, err := ledger.ParseLibraryFromJSON(js, ledger.GetEmbeddedFunctionResolver)
	if err != nil {
		return nil, err
	}
	h := lib.LibraryHash()
	return h[:], nil
}

// LibraryInfo is a library the dagstore knows and how it compares with this build's.
type LibraryInfo struct {
	UpgradeIndex   uint16 `json:"upgrade_index"`
	ActivationSlot uint32 `json:"activation_slot"`
	Hash           string `json:"hash"`
	Proven         bool   `json:"proven"`
	Check          string `json:"check"`
	Alarm          bool   `json:"alarm,omitempty"`
}

// Info lists the stored libraries and the build's not stored, by activation slot.
func (l *Libraries) Info() []LibraryInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	ret := make([]LibraryInfo, 0, len(l.built))
	have := make(map[uint32]bool)
	for _, s := range l.stored {
		have[s.ActivationSlot] = true
		info := LibraryInfo{UpgradeIndex: s.UpgradeIndex, ActivationSlot: s.ActivationSlot, Hash: hex.EncodeToString(s.Hash), Proven: true}
		switch bh, ok := l.builtHash[s.ActivationSlot]; {
		case s.ActivationSlot == 0:
			info.Check = "genesis"
		case !ok:
			info.Check = "unknown to this build: in use after a restart"
		case bytes.Equal(bh, s.Hash):
			info.Check = "matches this build"
		default:
			info.Check, info.Alarm = "ALARM: differs from this build's "+hex.EncodeToString(bh), true
		}
		ret = append(ret, info)
	}
	for slot := range l.built {
		if !have[slot] {
			ret = append(ret, LibraryInfo{ActivationSlot: slot, Hash: hex.EncodeToString(l.builtHash[slot]),
				Check: "this build's, in use; to be proven once active"})
		}
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].ActivationSlot < ret[j].ActivationSlot })
	for i := range ret {
		if !ret[i].Proven {
			ret[i].UpgradeIndex = ret[i-1].UpgradeIndex + 1 // the genesis is stored, so i > 0
		}
	}
	return ret
}

// Stored returns the stored libraries.
func (l *Libraries) Stored() []store.Library {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]store.Library(nil), l.stored...)
}

// Built returns this build's libraries by activation slot.
func (l *Libraries) Built() map[uint32][]byte { return l.built }

// Added records libraries stored after start.
func (l *Libraries) Added(libs []store.Library) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stored = append(l.stored, libs...)
	sort.Slice(l.stored, func(i, j int) bool { return l.stored[i].ActivationSlot < l.stored[j].ActivationSlot })
}

// Alarm raises an alarm, shown in /status and on the dashboard until restart; the same text once.
func (l *Libraries) Alarm(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.alarmIndex[msg] {
		l.alarmIndex[msg] = true
		l.alarms = append(l.alarms, msg)
	}
}

func (l *Libraries) Alarms() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.alarms...)
}

// Reconcile compares libraries fetched from the network, each already checked against its proof,
// with the stored ones and this build's. A fetched library this build makes the same is stored;
// one differing from the build's or from a stored one is an alarm and is not stored, since the
// network's library is the one that counts and this build must be upgraded; one the build does not
// list is stored, proven, and is an alarm too, because the cache takes it only after a restart.
func Reconcile(stored []store.Library, built map[uint32][]byte, builtHash map[uint32][]byte, fetched []store.Library) (toStore []store.Library, alarms []string) {
	have := make(map[uint32]store.Library)
	for _, s := range stored {
		have[s.ActivationSlot] = s
	}
	for _, f := range fetched {
		if s, ok := have[f.ActivationSlot]; ok {
			if !bytes.Equal(s.Hash, f.Hash) {
				alarms = append(alarms, fmt.Sprintf("the network's library of slot %d differs from the stored one", f.ActivationSlot))
			}
			continue
		}
		if _, ok := built[f.ActivationSlot]; !ok {
			toStore = append(toStore, f)
			alarms = append(alarms, fmt.Sprintf("the network has a library upgrade at slot %d this build does not list: "+
				"restart the dagstore to use it, or upgrade it if the upgrade changes Go code", f.ActivationSlot))
			continue
		}
		if !bytes.Equal(builtHash[f.ActivationSlot], f.Hash) {
			alarms = append(alarms, fmt.Sprintf("the network's library of slot %d differs from this build's: upgrade the dagstore", f.ActivationSlot))
			continue
		}
		toStore = append(toStore, f)
	}
	return toStore, alarms
}

// BuiltHash returns the hashes of this build's libraries by activation slot.
func (l *Libraries) BuiltHash() map[uint32][]byte { return l.builtHash }
