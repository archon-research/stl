package dexconsumer

import (
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type stubSQS struct{ outbound.SQSConsumer }
type stubMulticaller struct{ outbound.Multicaller }
type stubTxManager struct{ outbound.TxManager }
type stubTokenRepo struct{ outbound.TokenRepository }
type stubProtocolRepo struct{ outbound.ProtocolRepository }

func validCommonDeps() CommonDeps {
	return CommonDeps{
		SQSConsumer:  stubSQS{},
		CacheReader:  &fakeCache{},
		Multicaller:  stubMulticaller{},
		TxManager:    stubTxManager{},
		TokenRepo:    stubTokenRepo{},
		ProtocolRepo: stubProtocolRepo{},
		EventRepo:    &fakeEventRepo{},
	}
}

func TestCommonDeps_Validate_AllPresent(t *testing.T) {
	if err := validCommonDeps().Validate(); err != nil {
		t.Fatalf("Validate with all deps present: %v", err)
	}
}

func TestCommonDeps_Validate_RejectsEachMissingDep(t *testing.T) {
	cases := []struct {
		name string
		zero func(*CommonDeps)
		want string
	}{
		{"consumer", func(d *CommonDeps) { d.SQSConsumer = nil }, "consumer"},
		{"cache", func(d *CommonDeps) { d.CacheReader = nil }, "cache"},
		{"multicaller", func(d *CommonDeps) { d.Multicaller = nil }, "multicaller"},
		{"txManager", func(d *CommonDeps) { d.TxManager = nil }, "txManager"},
		{"tokenRepo", func(d *CommonDeps) { d.TokenRepo = nil }, "tokenRepo"},
		{"protocolRepo", func(d *CommonDeps) { d.ProtocolRepo = nil }, "protocolRepo"},
		{"eventRepo", func(d *CommonDeps) { d.EventRepo = nil }, "eventRepo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := validCommonDeps()
			tc.zero(&d)
			err := d.Validate()
			if err == nil {
				t.Fatalf("Validate with %s missing returned nil", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q should name the missing %s dependency", err, tc.want)
			}
		})
	}
}
