// Command dagstore-archive moves old history of a dagstore database into archive segments and
// back (SPEC.md, pruning to the archive):
//
//	dagstore-archive export -db <db> [-from <slot>] -to <slot> -out <dir>   write a segment
//	dagstore-archive verify -db <db> -segment <manifest>                    check a segment
//	dagstore-archive prune  -db <db> -segment <manifest>                    delete its range
//	dagstore-archive import -db <db> -segment <manifest> [-library_api <url> | -libraries_from <db>]
//	                                                                        build it into a database
//
// export reads beside a running dagstore. prune and import need the database to themselves: stop
// the dagstore first. verify needs a database only for the ledger's libraries.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/api"
	"github.com/lunfardo314/dagstore/archive"
	"github.com/lunfardo314/dagstore/live"
	"github.com/lunfardo314/dagstore/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dbPath := fs.String("db", "", "dagstore database")
	from := fs.Uint("from", 0, "export: first slot (default the starting branch's)")
	to := fs.Uint("to", 0, "export: last slot")
	out := fs.String("out", ".", "export: directory of the segment")
	segment := fs.String("segment", "", "the segment's manifest (.json)")
	libraryAPI := fs.String("library_api", "", "import into a new database: node API to fetch the libraries from, with their proofs")
	librariesFrom := fs.String("libraries_from", "", "import into a new database: another dagstore database to copy the libraries from")
	_ = fs.Parse(os.Args[2:])
	if *dbPath == "" {
		usage()
	}
	slog.Info(dagstore.BannerString())
	var err error
	switch cmd {
	case "export":
		err = export(*dbPath, uint32(*from), uint32(*to), *out)
	case "verify":
		err = verify(*dbPath, *segment)
	case "prune":
		err = prune(*dbPath, *segment)
	case "import":
		err = importSegment(*dbPath, *segment, *libraryAPI, *librariesFrom)
	default:
		usage()
	}
	if err != nil {
		slog.Error(cmd, "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: dagstore-archive export|verify|prune|import -db <db> [flags]; -h after the command lists them")
	os.Exit(2)
}

func export(dbPath string, from, to uint32, dir string) error {
	lock, err := store.LockShared(dbPath)
	if err != nil {
		return err
	}
	defer lock.Release()
	db, err := store.OpenReadOnly(dbPath, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	if from == 0 {
		if err = db.QueryRow(`SELECT t.slot FROM meta m JOIN tx t ON t.txid = m.value WHERE m.key = 'starting_branch'`).Scan(&from); err != nil {
			return fmt.Errorf("starting branch: %w", err)
		}
	}
	codec, err := store.LoadCodec(db)
	if err != nil {
		return err
	}
	m, err := archive.Export(db, codec, from, to, dir)
	if err != nil {
		return err
	}
	slog.Info("segment written", "file", filepath.Join(dir, m.File), "slots", fmt.Sprintf("%d..%d", m.FromSlot, m.ToSlot),
		"transactions", m.Count, "raw_bytes", m.RawBytes, "file_bytes", m.FileBytes, "lost", len(m.Lost))
	return nil
}

func verify(dbPath, manifest string) error {
	db, err := store.OpenReadOnly(dbPath, 1)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err = api.LoadLedger(db); err != nil {
		return err
	}
	m, err := archive.Verify(manifest)
	if err != nil {
		return err
	}
	slog.Info("segment verified", "file", m.File, "transactions", m.Count)
	return nil
}

func prune(dbPath, manifest string) error {
	lock, err := store.LockExclusive(dbPath)
	if err != nil {
		return err
	}
	defer lock.Release()
	db, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err = api.LoadLedger(db); err != nil {
		return err
	}
	return archive.Prune(db, manifest)
}

func importSegment(dbPath, manifest, libraryAPI, librariesFrom string) error {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) && librariesFrom != "" {
		if err = copyLibraries(librariesFrom, dbPath); err != nil {
			return err
		}
	}
	lock, err := store.LockExclusive(dbPath)
	if err != nil {
		return err
	}
	defer lock.Release()
	db, _, err := live.Open(dbPath, libraryAPI) // a new database gets its libraries, proven
	if err != nil {
		return err
	}
	defer db.Close()
	m, err := archive.Verify(manifest)
	if err != nil {
		return err
	}
	var genesis []byte
	if err = db.QueryRow(`SELECT lower(hex(hash)) FROM library WHERE activation_slot = 0`).Scan(&genesis); err != nil {
		return err
	}
	if string(genesis) != m.GenesisLibraryHash {
		return fmt.Errorf("segment of another ledger")
	}
	codec, err := store.LoadCodec(db)
	if err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(filepath.Dir(manifest), m.File))
	if err != nil {
		return err
	}
	defer f.Close()
	return live.ImportSegment(db, dagstore.DefaultParams, codec, m, f)
}

// copyLibraries creates a database holding the libraries of another one.
func copyLibraries(from, to string) error {
	src, err := store.OpenReadOnly(from, 1)
	if err != nil {
		return err
	}
	defer src.Close()
	libs, err := store.LoadLibraries(src)
	if err != nil {
		return err
	}
	db, err := store.Create(to)
	if err != nil {
		return err
	}
	defer db.Close()
	return store.SaveLibraries(db, libs)
}
