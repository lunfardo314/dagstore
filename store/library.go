package store

import (
	"database/sql"
	"fmt"
)

// Library is one row of the library table: a ledger library and the slot from which it applies.
// Stored with the data, the libraries keep the database readable without the binary of its time.
type Library struct {
	UpgradeIndex   uint16
	ActivationSlot uint32
	Hash           []byte
	JSON           []byte
}

// SaveLibraries writes the libraries, replacing any row of the same upgrade index.
func SaveLibraries(db *sql.DB, libs []Library) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for _, l := range libs {
		if _, err = tx.Exec(`INSERT OR REPLACE INTO library(upgrade_index, activation_slot, hash, json) VALUES (?,?,?,?)`,
			l.UpgradeIndex, l.ActivationSlot, l.Hash, l.JSON); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// LoadLibraries returns the stored libraries ordered by activation slot.
func LoadLibraries(db *sql.DB) ([]Library, error) {
	rows, err := db.Query(`SELECT upgrade_index, activation_slot, hash, json FROM library ORDER BY activation_slot`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ret []Library
	for rows.Next() {
		var l Library
		if err = rows.Scan(&l.UpgradeIndex, &l.ActivationSlot, &l.Hash, &l.JSON); err != nil {
			return nil, err
		}
		ret = append(ret, l)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(ret) == 0 || ret[0].ActivationSlot != 0 {
		return nil, fmt.Errorf("the database holds no genesis library")
	}
	return ret, nil
}
