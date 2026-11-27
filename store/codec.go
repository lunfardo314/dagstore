package store

import (
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/zstd"
)

// Transaction bytes are most of the database (SPEC.md, storage measured). Each blob is compressed on
// its own, since every read is of one transaction, with a zstd dictionary trained on the store's own
// first transactions: on real history that leaves 55% of the bytes, against 79% without one.
// A row of tx_bytes names the dictionary it was compressed with, NULL when stored raw, and a
// dictionary is never deleted, so a later one can be trained without touching older rows.

const (
	// DictTrainSamples is how many transactions are stored raw before the first dictionary is
	// trained from them.
	DictTrainSamples = 30_000
	dictMaxSize      = 112 << 10
	// zstd reserves dictionary IDs below 32768 for a public registry
	firstDictID = 32768
)

type Codec struct {
	mu    sync.Mutex // serialises adding a dictionary
	dicts [][]byte
	enc   atomic.Pointer[encoder]
	dec   atomic.Pointer[zstd.Decoder]
}

type encoder struct {
	id  int64
	enc *zstd.Encoder
}

// LoadCodec loads the stored dictionaries. A store without any stores raw until Train runs.
func LoadCodec(db *sql.DB) (*Codec, error) {
	c := &Codec{}
	rows, err := db.Query(`SELECT id, dict FROM zdict ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var d []byte
		if err = rows.Scan(&id, &d); err != nil {
			return nil, err
		}
		if err = c.add(id, d); err != nil {
			return nil, err
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if c.dec.Load() == nil {
		if err = c.setDecoder(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Codec) add(id int64, d []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression), zstd.WithEncoderDict(d),
		zstd.WithEncoderConcurrency(1), zstd.WithZeroFrames(true), zstd.WithEncoderCRC(false))
	if err != nil {
		return fmt.Errorf("dictionary %d: %w", id, err)
	}
	c.dicts = append(c.dicts, d)
	if err = c.setDecoder(); err != nil {
		return err
	}
	c.enc.Store(&encoder{id, enc})
	return nil
}

// setDecoder replaces the decoder by one that knows every dictionary: it finds the one a blob needs
// by the ID in the blob's frame header.
func (c *Codec) setDecoder() error {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(c.dicts...), zstd.WithDecoderConcurrency(1))
	if err != nil {
		return err
	}
	c.dec.Store(dec)
	return nil
}

// HasDict tells whether new rows are compressed.
func (c *Codec) HasDict() bool { return c.enc.Load() != nil }

// Encode returns what to store for raw, and the dictionary column: NULL while there is no
// dictionary yet.
func (c *Codec) Encode(raw []byte) (sql.NullInt64, []byte) {
	e := c.enc.Load()
	if e == nil {
		return sql.NullInt64{}, raw
	}
	return sql.NullInt64{Int64: e.id, Valid: true}, e.enc.EncodeAll(raw, nil)
}

// Decode returns the transaction bytes of a row of tx_bytes.
func (c *Codec) Decode(dictID sql.NullInt64, data []byte) ([]byte, error) {
	if !dictID.Valid {
		return data, nil
	}
	raw, err := c.dec.Load().DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress with dictionary %d: %w", dictID.Int64, err)
	}
	return raw, nil
}

// Execer is what the writers hold: the importer's and the live process's SQL transaction.
type Execer interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// TrainIfDue trains the first dictionary once DictTrainSamples transactions are stored raw, saves
// it and recompresses every raw row, all in the caller's SQL transaction. It does nothing once a
// dictionary exists, so its cost is one count over a small table while there is none.
func (c *Codec) TrainIfDue(tx Execer) error {
	if c.HasDict() {
		return nil
	}
	var n int
	if err := tx.QueryRow(`SELECT count(*) FROM tx_bytes`).Scan(&n); err != nil {
		return err
	}
	if n < DictTrainSamples {
		return nil
	}
	t := time.Now()
	type row struct {
		id  int64
		raw []byte
	}
	var rows []row
	var samples [][]byte
	r, err := tx.Query(`SELECT id, bytes FROM tx_bytes WHERE dict IS NULL ORDER BY id`)
	if err != nil {
		return err
	}
	for r.Next() {
		var x row
		if err = r.Scan(&x.id, &x.raw); err != nil {
			r.Close()
			return err
		}
		rows = append(rows, x)
		// training time grows with the sample, so a large raw backlog trains on its first part only
		if len(samples) < DictTrainSamples {
			samples = append(samples, x.raw)
		}
	}
	r.Close()
	if err = r.Err(); err != nil {
		return err
	}
	id := int64(firstDictID)
	d, err := dict.BuildZstdDict(samples, dict.Options{MaxDictSize: dictMaxSize, HashBytes: 6, ZstdDictID: uint32(id),
		ZstdLevel: zstd.SpeedBetterCompression})
	if err != nil {
		return fmt.Errorf("train dictionary: %w", err)
	}
	if _, err = tx.Exec(`INSERT INTO zdict(id, dict, trained_at, samples) VALUES (?, ?, ?, ?)`, id, d, time.Now().Unix(), len(samples)); err != nil {
		return err
	}
	if err = c.add(id, d); err != nil {
		return err
	}
	before, after := 0, 0
	for _, x := range rows {
		dictID, data := c.Encode(x.raw)
		if _, err = tx.Exec(`UPDATE tx_bytes SET dict = ?, bytes = ? WHERE id = ?`, dictID, data, x.id); err != nil {
			return err
		}
		before += len(x.raw)
		after += len(data)
	}
	slog.Info("compression dictionary trained", "id", id, "samples", len(samples), "dict_bytes", len(d),
		"ratio", fmt.Sprintf("%.3f", float64(after)/float64(max(1, before))), "took", time.Since(t).Round(time.Millisecond))
	return nil
}
