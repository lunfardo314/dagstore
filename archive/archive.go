// Package archive moves old history out of a dagstore database into archive segments (SPEC.md,
// pruning to the archive): a sealed slot range is exported to a segment, checked, and only then
// deleted from the database, the starting branch moving up past it. A segment holds transactions
// only, as the history walk needs them; importing it into any dagstore (package live) builds the
// branch table, the walks and the seals again, the way live sync does.
//
// A segment is two files: <name>.zst, the transactions of the range in txid (time) order, each as a
// 4-byte big-endian length and its bytes (the /export format), in one zstd stream; and <name>.json,
// its manifest.
package archive

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/transaction"
)

const FormatVersion = 1

// Manifest describes a segment. TxIDHash is SHA-256 over the txids of its transactions in order;
// Canonical lists the canonical branch of each slot that has one, in slot order, which is what an
// import needs to number the lineage without feeders.
type Manifest struct {
	Format             int       `json:"format"`
	File               string    `json:"file"`
	FromSlot           uint32    `json:"from_slot"`
	ToSlot             uint32    `json:"to_slot"`
	GenesisLibraryHash string    `json:"genesis_library_hash"`
	Count              int64     `json:"count"`
	RawBytes           int64     `json:"raw_bytes"`
	FileBytes          int64     `json:"file_bytes"`
	FileSHA256         string    `json:"file_sha256"`
	TxIDHash           string    `json:"txid_hash"`
	Canonical          []string  `json:"canonical"`
	Lost               []string  `json:"lost"`
	CreatedAt          time.Time `json:"created_at"`
}

// Name is the base name of a segment's files.
func Name(from, to uint32) string { return fmt.Sprintf("segment-%d-%d", from, to) }

// Export writes the segment of slots from..to into dir. The range must be sealed: every branch in
// it sealed, nothing in it still pending a pull, and the newest sealed canonical branch above it.
func Export(db *sql.DB, codec *store.Codec, from, to uint32, dir string) (*Manifest, error) {
	if err := CheckSealed(db, from, to); err != nil {
		return nil, err
	}
	m := &Manifest{Format: FormatVersion, File: Name(from, to) + ".zst", FromSlot: from, ToSlot: to, CreatedAt: time.Now().UTC(),
		Canonical: []string{}, Lost: []string{}}
	var genesis []byte
	if err := db.QueryRow(`SELECT hash FROM library WHERE activation_slot = 0`).Scan(&genesis); err != nil {
		return nil, fmt.Errorf("genesis library: %w", err)
	}
	m.GenesisLibraryHash = hex.EncodeToString(genesis)
	var err error
	if m.Canonical, err = hexColumn(db, `SELECT t.txid FROM branch b JOIN tx t ON t.id = b.tx
		WHERE b.canonical_no IS NOT NULL AND b.slot BETWEEN ? AND ? ORDER BY b.canonical_no`, from, to); err != nil {
		return nil, err
	}
	if m.Lost, err = hexColumn(db, `SELECT txid FROM tx WHERE txid >= ? AND txid < ? AND state = ? ORDER BY txid`,
		slotKey(from), slotKey(to+1), store.StateLost); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, m.File)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fileHash := sha256.New()
	counted := &countingWriter{w: io.MultiWriter(f, fileHash)}
	zw, err := zstd.NewWriter(counted, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(zw, 1<<20)
	txidHash := sha256.New()
	rows, err := db.Query(`SELECT t.txid, b.bytes, b.dict FROM tx t JOIN tx_bytes b ON b.id = t.id
		WHERE t.txid >= ? AND t.txid < ? ORDER BY t.txid`, slotKey(from), slotKey(to+1))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lenBuf [4]byte
	for rows.Next() {
		var txid, data []byte
		var dict sql.NullInt64
		if err = rows.Scan(&txid, &data, &dict); err != nil {
			return nil, err
		}
		raw, err := codec.Decode(dict, data)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(raw)))
		if _, err = bw.Write(lenBuf[:]); err == nil {
			_, err = bw.Write(raw)
		}
		if err != nil {
			return nil, err
		}
		txidHash.Write(txid)
		m.Count++
		m.RawBytes += int64(len(raw))
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = bw.Flush(); err != nil {
		return nil, err
	}
	if err = zw.Close(); err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	m.FileBytes, m.FileSHA256, m.TxIDHash = counted.n, hex.EncodeToString(fileHash.Sum(nil)), hex.EncodeToString(txidHash.Sum(nil))
	return m, writeManifest(filepath.Join(dir, Name(from, to)+".json"), m)
}

// CheckSealed refuses a range that is not final yet: an unsealed branch in it, a transaction in it
// still pending a pull, or no sealed canonical branch above it to become the starting branch.
func CheckSealed(db *sql.DB, from, to uint32) error {
	if to < from {
		return fmt.Errorf("slot range %d..%d is empty", from, to)
	}
	var unsealed, pending int64
	if err := db.QueryRow(`SELECT count(*) FROM branch WHERE slot BETWEEN ? AND ? AND sealed = 0`, from, to).Scan(&unsealed); err != nil {
		return err
	}
	if err := db.QueryRow(`SELECT count(*) FROM tx WHERE txid >= ? AND txid < ? AND state = ?`, slotKey(from), slotKey(to+1), store.StateMissing).
		Scan(&pending); err != nil {
		return err
	}
	if unsealed > 0 || pending > 0 {
		return fmt.Errorf("slots %d..%d are not final: %d branches unsealed, %d transactions pending a pull", from, to, unsealed, pending)
	}
	var above int64
	if err := db.QueryRow(`SELECT count(*) FROM branch WHERE slot > ? AND canonical_no IS NOT NULL AND sealed = 1`, to).Scan(&above); err != nil {
		return err
	}
	if above == 0 {
		return fmt.Errorf("no sealed canonical branch above slot %d", to)
	}
	return nil
}

// Records reads a segment's transactions in order, calling fun with each one's bytes.
func Records(r io.Reader, fun func(raw []byte) error) error {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return err
	}
	defer zr.Close()
	br := bufio.NewReaderSize(zr, 1<<20)
	var lenBuf [4]byte
	for {
		if _, err = io.ReadFull(br, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("segment truncated: %w", err)
		}
		raw := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
		if _, err = io.ReadFull(br, raw); err != nil {
			return fmt.Errorf("segment truncated: %w", err)
		}
		if err = fun(raw); err != nil {
			return err
		}
	}
}

// Verify checks a segment against its manifest: the file's hash, and every transaction parsed, in
// strict txid order, inside the slot range, the count and the txid hash as stated. Parsing needs
// the ledger's libraries loaded.
func Verify(manifestPath string) (*Manifest, error) {
	m, err := ReadManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	if m.Format != FormatVersion {
		return nil, fmt.Errorf("segment format %d, this build reads %d", m.Format, FormatVersion)
	}
	f, err := os.Open(filepath.Join(filepath.Dir(manifestPath), m.File))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fileHash := sha256.New()
	txidHash := sha256.New()
	var count, rawBytes int64
	var prev []byte
	err = Records(io.TeeReader(f, fileHash), func(raw []byte) error {
		tx, err := transaction.Parse(raw)
		if err != nil {
			return fmt.Errorf("transaction %d: %w", count, err)
		}
		id := tx.ID()
		if prev != nil && string(id[:]) <= string(prev) {
			return fmt.Errorf("transaction %d out of order", count)
		}
		if s := id.Slot(); s < m.FromSlot || s > m.ToSlot {
			return fmt.Errorf("transaction %s outside slots %d..%d", id.StringShort(), m.FromSlot, m.ToSlot)
		}
		prev = append(prev[:0], id[:]...)
		txidHash.Write(id[:])
		count++
		rawBytes += int64(len(raw))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if _, err = io.Copy(fileHash, f); err != nil { // anything after the zstd frames
		return nil, err
	}
	for _, c := range []struct {
		what      string
		got, want string
	}{
		{"file hash", hex.EncodeToString(fileHash.Sum(nil)), m.FileSHA256},
		{"txid hash", hex.EncodeToString(txidHash.Sum(nil)), m.TxIDHash},
		{"count", fmt.Sprint(count), fmt.Sprint(m.Count)},
		{"raw bytes", fmt.Sprint(rawBytes), fmt.Sprint(m.RawBytes)},
	} {
		if c.got != c.want {
			return nil, fmt.Errorf("segment %s: %s %s, manifest says %s", m.File, c.what, c.got, c.want)
		}
	}
	return m, nil
}

func ReadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	m := &Manifest{}
	return m, json.Unmarshal(data, m)
}

func writeManifest(path string, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// slotKey is the smallest txid of a slot: a txid begins with its big-endian slot.
func slotKey(slot uint32) []byte { return base.Slot2Bytes(slot) }

func hexColumn(db *sql.DB, query string, args ...any) ([]string, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := []string{}
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		ret = append(ret, hex.EncodeToString(b))
	}
	return ret, rows.Err()
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
