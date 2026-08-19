package main

import (
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// TestExtractCandidatesFromReceipts_CorruptMorphoBlueLogFailsRun locks in the
// backstop that mirrors the live indexer: a log whose topic0 is a recognised
// Morpho Blue event but whose body fails to decode is a hard error, not a
// silently dropped candidate.
//
// In the live indexer every Morpho Blue log from the singleton is re-extracted
// by processMorphoBlueLog (service.go), which returns an error on decode
// failure; the discovery pre-walk's WARN+continue is safe only because that
// second pass exists. The backfiller has no second pass, so
// emitMorphoBlueCandidates must itself hard-error — otherwise a corrupt log
// silently thins the discovered vault set (and, downstream, the V2 replay that
// runs off the vaults this pipeline persists).
func TestExtractCandidatesFromReceipts_CorruptMorphoBlueLogFailsRun(t *testing.T) {
	t.Parallel()

	extractor, err := morpho_indexer.NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor: %v", err)
	}

	morphoBlueABI, err := abis.GetMorphoBlueEventsABI()
	if err != nil {
		t.Fatalf("GetMorphoBlueEventsABI: %v", err)
	}
	supplyTopic0 := morphoBlueABI.Events["Supply"].ID.Hex()

	// Recognised topic0 (IsMorphoBlueEvent == true) but only topic0 present:
	// Supply declares three indexed params, so ExtractMorphoBlueEvent fails in
	// parseTopics with a topic-count mismatch.
	corruptLog := shared.Log{
		Address:         morpho_indexer.MorphoBlueAddress.Hex(),
		Topics:          []string{supplyTopic0},
		TransactionHash: "0xabc",
	}
	if !extractor.IsMorphoBlueEvent(corruptLog) {
		t.Fatal("precondition: corrupt log must be a recognised Morpho Blue event")
	}
	if _, decodeErr := extractor.ExtractMorphoBlueEvent(corruptLog); decodeErr == nil {
		t.Fatal("precondition: corrupt log must fail to decode")
	}

	receipts := []shared.TransactionReceipt{{Logs: []shared.Log{corruptLog}}}
	candidateCh := make(chan candidateEntry, 8)

	if err := extractCandidatesFromReceipts(receipts, extractor, morpho_indexer.MorphoBlueAddress, 123, candidateCh); err == nil {
		t.Fatal("expected a recognised-but-undecodable Morpho Blue log to fail the run, got nil")
	}
}
