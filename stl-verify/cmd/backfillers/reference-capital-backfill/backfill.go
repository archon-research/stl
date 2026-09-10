package main

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/skydata"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/writerrun"
	"github.com/archon-research/stl/stl-verify/internal/services/reference_capital_backfill"
)

// maxDaysAgo bounds a mistyped window. Over-asking is harmless on its own — the
// feed holds roughly a year and answers a longer request with what it has — but
// an unbounded field turns a fat-fingered "3650000" into a pointless request for
// ten millennia of history.
const maxDaysAgo = 3650

// BackfillParams is the JSON an operator supplies in the Temporal UI's Input box:
//
//	{"daysAgo": 365}
//
// The feed is queried by relative age rather than an absolute window, because
// that is the only shape its history route accepts.
type BackfillParams struct {
	DaysAgo int `json:"daysAgo"`
}

// validate takes no clock, unlike its offchain-price-backfill counterpart: a
// relative day count needs no comparison against now, so there is nothing here
// that could diverge across a workflow replay.
func (p BackfillParams) validate() error {
	if p.DaysAgo <= 0 {
		return fmt.Errorf("daysAgo must be a positive number of days (e.g. 365); got %d", p.DaysAgo)
	}
	if p.DaysAgo > maxDaysAgo {
		return fmt.Errorf("daysAgo %d exceeds the %d-day ceiling; the feed holds about a year", p.DaysAgo, maxDaysAgo)
	}
	return nil
}

// BackfillResult is the workflow's return value, shown in the UI's Result panel.
// It names the primes seeded rather than a row count: the service refuses a
// partial backfill outright, so "these primes, this window" is the whole outcome.
type BackfillResult struct {
	DaysAgo int      `json:"daysAgo"`
	Primes  []string `json:"primes"`
}

// backfillWorkflow runs the seed as a single activity.
//
// Deliberately not chunked per prime, unlike offchain-price-backfill. The
// service fetches every tracked prime in one request and refuses to write unless
// all of them came back (requireEveryStarCovered), because the write is
// ON CONFLICT DO NOTHING and a prime missing from a one-shot seed leaves a
// permanent hole a re-run cannot repair. Splitting that across activities would
// let some primes commit while others failed — exactly the partial state the
// service exists to prevent.
func backfillWorkflow(ctx workflow.Context, params BackfillParams) (BackfillResult, error) {
	logger := workflow.GetLogger(ctx)

	if err := params.validate(); err != nil {
		// Bad input fails identically on every attempt, so retrying it would only
		// bury the mistake under the retry envelope.
		return BackfillResult{}, temporalsdk.NewNonRetryableApplicationError(
			"invalid backfill parameters", "InvalidParams", err)
	}

	logger.Info("starting reference balance-sheet backfill", "daysAgo", params.DaysAgo)

	ctx = workflow.WithActivityOptions(ctx, backfillActivityOptions())

	var activities *backfillActivities
	var primes []string
	if err := workflow.ExecuteActivity(ctx, activities.Backfill, params.DaysAgo).Get(ctx, &primes); err != nil {
		return BackfillResult{}, err
	}

	logger.Info("reference balance-sheet backfill complete", "daysAgo", params.DaysAgo, "primes", primes)
	return BackfillResult{DaysAgo: params.DaysAgo, Primes: primes}, nil
}

func backfillActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		// One attempt covers a year of history for every tracked prime: one feed
		// request, then a single batched insert of roughly days x primes rows.
		StartToCloseTimeout: 15 * time.Minute,

		// Total time INCLUDING retries, which is what bounds the run rather than a
		// small attempt cap — an attempt cap turns slow-but-progressing work into a
		// hard failure, whereas an envelope lets a transient upstream blip retry
		// while still refusing to hang forever.
		ScheduleToCloseTimeout: 45 * time.Minute,

		RetryPolicy: &temporalsdk.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			// No MaximumAttempts — ScheduleToCloseTimeout above is the bound. Retrying
			// is safe because the write is insert-only and conflicts away within a
			// build, so a retry after a partial write converges rather than doubling.
		},
	}
}

// backfillActivities holds the run-invariant dependencies. daysAgo is not among
// them: it is per-run, so the service is built at activity time around the value
// the operator supplied.
type backfillActivities struct {
	newService func(daysAgo int) *reference_capital_backfill.Service
	primes     []string
}

// Backfill seeds the balance-sheet history and returns the primes it covered.
func (a *backfillActivities) Backfill(ctx context.Context, daysAgo int) ([]string, error) {
	if err := a.newService(daysAgo).Run(ctx); err != nil {
		return nil, err
	}
	return a.primes, nil
}

func newBackfillActivities(ctx context.Context, deps temporal.Dependencies) (*backfillActivities, error) {
	buildReg, runID, err := writerrun.Open(ctx, deps.Pool)
	if err != nil {
		return nil, err
	}

	txm, err := postgres.NewTxManager(deps.Pool, deps.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating tx manager: %w", err)
	}

	client, err := skydata.NewClient(skydata.ClientConfig{
		BaseURL: env.Get("SKY_DATA_URL", defaultSkyDataURL),
		Logger:  deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating sky-data client: %w", err)
	}

	primes, err := trackedStarsFromContract()
	if err != nil {
		return nil, err
	}

	primeRepo := postgres.NewPrimeRepository(deps.Pool)
	sheetRepo := postgres.NewPrimeBalanceSheetRepository(deps.Pool, txm, deps.Logger, runID)
	buildID := int(buildReg.BuildID())
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &backfillActivities{
		primes: primes,
		newService: func(daysAgo int) *reference_capital_backfill.Service {
			return reference_capital_backfill.NewService(
				primeRepo, sheetRepo, client, primes, daysAgo, buildID, logger,
			)
		},
	}, nil
}

// trackedStarsFromContract names the primes STL tracks, sorted for a stable
// request order.
func trackedStarsFromContract() ([]string, error) {
	contract, err := axis_synome_contract.LoadDefaultContract()
	if err != nil {
		return nil, fmt.Errorf("loading axis-synome contract: %w", err)
	}

	almProxies := contract.GetAlmProxies()
	stars := make([]string, 0, len(almProxies))
	for star := range almProxies {
		stars = append(stars, star)
	}
	if len(stars) == 0 {
		return nil, fmt.Errorf("axis-synome contract names no primes")
	}
	sort.Strings(stars)
	return stars, nil
}
