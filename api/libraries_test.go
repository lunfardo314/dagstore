package api

import (
	"strings"
	"testing"

	"github.com/lunfardo314/dagstore/store"
)

// Reconcile decides, for libraries fetched from the network with their proofs, which to store and
// which to raise an alarm on. Only hashes and presence matter, so the libraries here are stand-ins.
func TestReconcile(t *testing.T) {
	h := func(b byte) []byte { return []byte{b, b, b} }
	lib := func(slot uint32, hash byte) store.Library {
		return store.Library{ActivationSlot: slot, Hash: h(hash), JSON: []byte{hash}}
	}
	stored := []store.Library{lib(0, 1), lib(100, 2)}
	// this build lists genesis, the stored upgrade and two more, at 200 and 300
	built := map[uint32][]byte{0: {1}, 100: {2}, 200: {3}, 300: {4}}
	builtHash := map[uint32][]byte{0: h(1), 100: h(2), 200: h(3), 300: h(4)}

	fetched := []store.Library{
		lib(0, 1),   // stored, same: nothing to do
		lib(100, 9), // stored, but the network's differs: alarm, nothing stored
		lib(200, 3), // the build's, same: stored, now proven
		lib(300, 8), // the build's, but the network's differs: alarm, not stored
		lib(400, 5), // not listed by this build: stored, proven, with an alarm to restart or upgrade
	}
	toStore, alarms := Reconcile(stored, built, builtHash, fetched)

	var slots []uint32
	for _, l := range toStore {
		slots = append(slots, l.ActivationSlot)
	}
	if len(slots) != 2 || slots[0] != 200 || slots[1] != 400 {
		t.Errorf("stored %v, want [200 400]", slots)
	}
	want := []string{"slot 100 differs from the stored one", "slot 300 differs from this build's", "slot 400 this build does not list"}
	if len(alarms) != len(want) {
		t.Fatalf("alarms %q, want %d", alarms, len(want))
	}
	for i, w := range want {
		if !strings.Contains(alarms[i], w) {
			t.Errorf("alarm %d: %q, want it to say %q", i, alarms[i], w)
		}
	}

	// once stored, a second fetch of the same libraries changes nothing and raises nothing new
	toStore, alarms = Reconcile(append(stored, lib(200, 3), lib(400, 5)), built, builtHash, fetched[2:3])
	if len(toStore) != 0 || len(alarms) != 0 {
		t.Errorf("second fetch: stored %d, alarms %q", len(toStore), alarms)
	}
}
