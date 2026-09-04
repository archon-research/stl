// Package archiveblock reads the block hash an archived block payload carries.
// Both the bulk downloader and the block republisher compare it against the
// canonical chain to decide whether a height needs repairing at all, so the fold
// — which object answers, in what order, and what "no hash" means — lives here
// rather than in either tool.
package archiveblock

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// PrefixBytes is how much of an archived object is read: the hash sits in the
// first JSON fields, so a prefix answers for an object of megabytes.
const PrefixBytes = 8 << 10

// RangeReader is the partial read this package needs; outbound.S3RangeReader
// satisfies it.
type RangeReader interface {
	ReadRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error)
}

// hashSource is where an archived object carries the block hash: the block
// payload names it at the top level, a receipt one frame deeper.
type hashSource struct {
	DataType s3key.DataType
	Depth    int
	Field    string
}

var hashSources = []hashSource{
	{s3key.Block, 1, "hash"},
	{s3key.Receipts, 2, "blockHash"},
}

// ErrUnreadable marks an archived object no attempt can read: not a gzip stream,
// a corrupt deflate stream, no bytes at all, or a hash beyond the ranged prefix.
// Retrying any of them burns the whole envelope on a verdict that cannot change.
var ErrUnreadable = errors.New("archived object cannot be read")

// Hash returns the block hash the archive holds at (blockNumber, version), from
// the block object or else the receipts. found is false when neither object is
// there and when neither carries a hash: a zero-tx block's empty receipt list
// and a null payload identify no block, and neither may fail the height on every
// run. A read that fails, an object that will not decompress, and a hash the
// prefix could not reach are errors — reading any of them as "no hash" would
// repair a height whose archive is already canonical.
func Hash(ctx context.Context, reader RangeReader, bucket string, blockNumber int64, version int) (string, bool, error) {
	part := partition.GetPartition(blockNumber)

	for _, source := range hashSources {
		key := s3key.BuildWithPartition(part, blockNumber, version, source.DataType)
		hash, err := hashFromObject(ctx, reader, bucket, key, source)
		if errors.Is(err, outbound.ErrObjectNotFound) {
			continue
		}
		if err != nil {
			return "", false, err
		}
		if hash != "" {
			return hash, true, nil
		}
	}
	return "", false, nil
}

// HashFromPayload returns the hash a block payload carries, for a document
// already in hand rather than in the archive.
func HashFromPayload(payload []byte) (string, bool) {
	hash, outcome := scanStringField(payload, 1, "hash")
	return hash, outcome == fieldFound
}

func hashFromObject(ctx context.Context, reader RangeReader, bucket, key string, source hashSource) (string, error) {
	stored, err := reader.ReadRange(ctx, bucket, key, 0, PrefixBytes-1)
	if errors.Is(err, outbound.ErrObjectEmpty) {
		return "", fmt.Errorf("reading %s: %w: %w", key, err, ErrUnreadable)
	}
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", key, err)
	}

	plain, err := gunzipPrefix(stored)
	if err != nil {
		return "", fmt.Errorf("decompressing %s: %w: %w", key, err, ErrUnreadable)
	}

	hash, outcome := scanStringField(plain, source.Depth, source.Field)
	if outcome == fieldTruncated {
		return "", fmt.Errorf("no %s in the first %d bytes of %s: %w", source.Field, PrefixBytes, key, ErrUnreadable)
	}
	return hash, nil
}

// gunzipPrefix decompresses what it can of a truncated gzip stream: the
// unexpected EOF that ends a ranged read is the expected outcome here.
func gunzipPrefix(stored []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(stored))
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	plain, err := io.ReadAll(gz)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return plain, nil
}

// fieldOutcome tells a document that carries no such field from a prefix that
// ended before the field could appear.
type fieldOutcome int

const (
	fieldFound fieldOutcome = iota
	fieldAbsent
	fieldTruncated
)

// scanStringField looks for the first string value of the named field at the
// given object depth. A complete document that closed without the field is
// fieldAbsent; one whose last token ran into the end of a truncated prefix is
// fieldTruncated.
func scanStringField(doc []byte, depth int, field string) (string, fieldOutcome) {
	dec := json.NewDecoder(bytes.NewReader(doc))
	var objectFrames []bool
	expectKey, wanted, seen := false, false, false

	for {
		token, err := dec.Token()
		if err != nil {
			if seen && len(objectFrames) == 0 && errors.Is(err, io.EOF) {
				return "", fieldAbsent
			}
			return "", fieldTruncated
		}
		seen = true

		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				objectFrames = append(objectFrames, true)
				expectKey = true
			case '[':
				objectFrames = append(objectFrames, false)
				expectKey = false
			default:
				objectFrames = objectFrames[:len(objectFrames)-1]
				expectKey = len(objectFrames) > 0 && objectFrames[len(objectFrames)-1]
			}
			wanted = false
			continue
		}

		if len(objectFrames) == 0 || !objectFrames[len(objectFrames)-1] {
			continue
		}
		if expectKey {
			key, _ := token.(string)
			wanted = len(objectFrames) == depth && key == field
			expectKey = false
			continue
		}

		expectKey = true
		if wanted {
			value, ok := token.(string)
			if !ok {
				return "", fieldAbsent
			}
			return value, fieldFound
		}
	}
}
