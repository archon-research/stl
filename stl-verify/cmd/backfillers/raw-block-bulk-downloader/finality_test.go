package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
)

// fakeFinalizedHead answers the one call the finality guard makes.
type fakeFinalizedHead struct {
	head  int64
	err   error
	calls int
}

func (f *fakeFinalizedHead) GetFinalizedBlockNumber(context.Context) (int64, error) {
	f.calls++
	return f.head, f.err
}

func TestGuardFinality_RefusesAnEndBlockAboveTheFinalizedHead(t *testing.T) {
	node := &fakeFinalizedHead{head: 25395650}
	logger, _ := captureLogger()

	err := guardFinality(context.Background(), node, Config{StartBlock: 25395000, EndBlock: 25395651}, logger)

	if err == nil {
		t.Fatal("expected the run refused: a losing fork archived above the finalized head can never be corrected")
	}
	for _, want := range []string{strconv.FormatInt(25395651, 10), strconv.FormatInt(node.head, 10)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s; an operator needs both numbers", err, want)
		}
	}
}

func TestGuardFinality_AllowsAnEndBlockAtTheFinalizedHead(t *testing.T) {
	logger, _ := captureLogger()

	err := guardFinality(context.Background(), &fakeFinalizedHead{head: 25395651}, Config{StartBlock: 25395000, EndBlock: 25395651}, logger)

	if err != nil {
		t.Fatalf("guardFinality() error = %v, want the finalized head itself archivable", err)
	}
}

func TestGuardFinality_AllowUnfinalizedOverridesTheRefusal(t *testing.T) {
	node := &fakeFinalizedHead{head: 25395650}
	logger, logs := captureLogger()

	err := guardFinality(context.Background(), node, Config{StartBlock: 25395000, EndBlock: 25395651, AllowUnfinalized: true}, logger)

	if err != nil {
		t.Fatalf("guardFinality() error = %v, want --allow-unfinalized to override the guard", err)
	}
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("logged %q, want the override at WARN", logs.String())
	}
}

func TestGuardFinality_FailsClosedWhenTheNodeDoesNotServeTheFinalizedTag(t *testing.T) {
	node := &fakeFinalizedHead{err: errors.New("the method eth_getBlockByNumber does not exist")}
	logger, _ := captureLogger()

	err := guardFinality(context.Background(), node, Config{StartBlock: 25395000, EndBlock: 25395651}, logger)

	if err == nil {
		t.Fatal("expected the run refused: an unreadable finalized head must not silently skip the guard")
	}
	if !strings.Contains(err.Error(), "finalized") {
		t.Errorf("error %q does not name the finalized tag the node must serve", err)
	}
}
