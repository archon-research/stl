package dexbootstrap

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// N8-1: dexbootstrap.loadAWSConfig defaulted to "us-east-1" while every other
// worker uses awsconfig.DefaultRegion ("eu-west-1") via the shared
// internal/pkg/awsconfig package. The fix is to call awsconfig.Load so the
// default is shared. Test asserts the unset-AWS_REGION default lands on
// eu-west-1, matching every other worker.
func TestLoadAWSConfig_DefaultsToEUWest1WhenAWSRegionUnset(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	cfg, err := loadAWSConfig(context.Background())
	if err != nil {
		t.Fatalf("loadAWSConfig: %v", err)
	}
	if cfg.Region != "eu-west-1" {
		t.Fatalf("Region = %q, want eu-west-1 (matching awsconfig.DefaultRegion; every stl- deployment lives there)", cfg.Region)
	}
}

// N8-2: when AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is empty,
// fail loudly rather than silently authenticate with a half-populated set.
// awsconfig.Load already does this; the inline copy in dexbootstrap did not.
func TestLoadAWSConfig_RejectsAKIDWithoutSecret(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKID-without-secret")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	_, err := loadAWSConfig(context.Background())
	if err == nil {
		t.Fatal("loadAWSConfig: expected error when AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is empty")
	}
	if !strings.Contains(err.Error(), "AWS_SECRET_ACCESS_KEY") {
		t.Errorf("error %q must reference AWS_SECRET_ACCESS_KEY so operator can fix the right var", err)
	}
}

// N8: the BootstrapOptions guards run before any AWS/network setup, so a worker
// that forgets to set its identity crashes loudly at boot rather than emitting
// unlabelled metrics or an empty service name.
func TestBootstrap_RejectsEmptyServiceName(t *testing.T) {
	_, err := Bootstrap(context.Background(), Config{}, BootstrapOptions{ServiceName: "", MetricPrefix: "curve"})
	if err == nil {
		t.Fatal("Bootstrap with empty ServiceName must return an error")
	}
	if !strings.Contains(err.Error(), "ServiceName") {
		t.Errorf("error %q should name the missing ServiceName option", err)
	}
}

func TestBootstrap_RejectsEmptyMetricPrefix(t *testing.T) {
	_, err := Bootstrap(context.Background(), Config{}, BootstrapOptions{ServiceName: "curve-dex-worker", MetricPrefix: ""})
	if err == nil {
		t.Fatal("Bootstrap with empty MetricPrefix must return an error")
	}
	if !strings.Contains(err.Error(), "MetricPrefix") {
		t.Errorf("error %q should name the missing MetricPrefix option", err)
	}
}

// N8: Close must tear resources down in reverse registration order so each
// resource is released before the one it depends on (e.g. OTEL flush before the
// Postgres pool it may reference).
func TestDepsClose_RunsCleanupsInReverseOrder(t *testing.T) {
	var order []int
	d := &Deps{}
	for i := 1; i <= 3; i++ {
		d.cleanups = append(d.cleanups, func() { order = append(order, i) })
	}

	d.Close()

	if want := []int{3, 2, 1}; !slices.Equal(order, want) {
		t.Errorf("cleanup order = %v, want %v (reverse of registration)", order, want)
	}
}

// stubBlockNumberer is a blockNumberer double returning a preset head or error.
type stubBlockNumberer struct {
	head uint64
	err  error
}

func (s stubBlockNumberer) BlockNumber(context.Context) (uint64, error) {
	return s.head, s.err
}

func TestDeps_LatestBlock(t *testing.T) {
	tests := []struct {
		name    string
		stub    stubBlockNumberer
		want    *big.Int
		wantErr bool
	}{
		{name: "returns head as big.Int", stub: stubBlockNumberer{head: 21_000_000}, want: big.NewInt(21_000_000)},
		{name: "wraps client error", stub: stubBlockNumberer{err: fmt.Errorf("rpc down")}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Deps{blockNumberer: tt.stub}
			got, err := d.LatestBlock(context.Background())
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("LatestBlock: %v", err)
			}
			if got.Cmp(tt.want) != 0 {
				t.Errorf("LatestBlock = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestDeps_LatestBlock_ErrorsWhenNumbererUninitialised(t *testing.T) {
	d := &Deps{}
	if _, err := d.LatestBlock(context.Background()); err == nil {
		t.Fatal("LatestBlock with nil blockNumberer must error, got nil")
	}
}

// Distinct stub types per port so a transposed mapping in CommonDeps would
// compare unequal (interface comparison includes the dynamic type).
type stubSQSConsumer struct{ outbound.SQSConsumer }
type stubCacheReader struct{ outbound.BlockCacheReader }
type stubMulticaller struct{ outbound.Multicaller }
type stubTxManager struct{ outbound.TxManager }
type stubTokenRepo struct{ outbound.TokenRepository }
type stubProtocolRepo struct{ outbound.ProtocolRepository }
type stubEventRepo struct{ outbound.EventRepository }

func TestDeps_CommonDeps_MapsEveryPort(t *testing.T) {
	consumer := stubSQSConsumer{}
	cache := stubCacheReader{}
	mc := stubMulticaller{}
	txm := stubTxManager{}
	tok := stubTokenRepo{}
	proto := stubProtocolRepo{}
	evt := stubEventRepo{}

	d := &Deps{
		SQSConsumer:  consumer,
		CacheReader:  cache,
		Multicaller:  mc,
		TxManager:    txm,
		TokenRepo:    tok,
		ProtocolRepo: proto,
		EventRepo:    evt,
	}

	cd := d.CommonDeps()
	if cd.SQSConsumer != consumer || cd.CacheReader != cache || cd.Multicaller != mc ||
		cd.TxManager != txm || cd.TokenRepo != tok || cd.ProtocolRepo != proto || cd.EventRepo != evt {
		t.Fatal("CommonDeps did not map every port through identically")
	}
	if err := cd.Validate(); err != nil {
		t.Errorf("a fully-wired CommonDeps must validate: %v", err)
	}
}
