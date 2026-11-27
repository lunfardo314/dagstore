package store

import (
	"fmt"
	"os"
	"syscall"
)

// Lock holds a lock on <db>.lock beside the database. Every process that opens a database holds it
// shared; pruning, which deletes ranges the others may be reading or walking from, holds it
// exclusive, so it runs only while nothing else has the database open.
type Lock struct{ f *os.File }

// LockShared takes the shared lock, failing at once if pruning holds it.
func LockShared(dbPath string) (*Lock, error) { return lock(dbPath, syscall.LOCK_SH) }

// LockExclusive takes the exclusive lock, failing at once if any process has the database open.
func LockExclusive(dbPath string) (*Lock, error) { return lock(dbPath, syscall.LOCK_EX) }

func lock(dbPath string, how int) (*Lock, error) {
	f, err := os.OpenFile(dbPath+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if how == syscall.LOCK_EX {
			return nil, fmt.Errorf("%s is open in another process: stop it first", dbPath)
		}
		return nil, fmt.Errorf("%s is being pruned", dbPath)
	}
	return &Lock{f}, nil
}

func (l *Lock) Release() { _ = l.f.Close() }
