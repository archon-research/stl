package allocation_tracker

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// TestLoadDefaultProxies_KeepsAlmAndSubProxy guards against a regression where
// the axis-synome export collapsed each (star, chain) to a single proxy,
// dropping the SubProxy/treasury wallets. Both the ALM proxy and the SubProxy
// must be present for the mainnet stars.
func TestLoadDefaultProxies_KeepsAlmAndSubProxy(t *testing.T) {
	t.Parallel()

	proxies, err := LoadDefaultProxies()
	if err != nil {
		t.Fatalf("LoadDefaultProxies() error = %v", err)
	}

	byStarChain := make(map[[2]string]map[common.Address]string)
	for _, p := range proxies {
		key := [2]string{p.Star, p.Chain}
		if byStarChain[key] == nil {
			byStarChain[key] = make(map[common.Address]string)
		}
		byStarChain[key][p.Address] = p.Role
	}

	cases := []struct {
		star, chain string
		want        map[common.Address]string // address -> expected role
	}{
		{
			star:  "spark",
			chain: "mainnet",
			want: map[common.Address]string{
				common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E"): "alm",
				common.HexToAddress("0x3300f198988e4c9c63f75df86de36421f06af8c4"): "subproxy",
			},
		},
		{
			star:  "grove",
			chain: "mainnet",
			want: map[common.Address]string{
				common.HexToAddress("0x491EDFB0B8b608044e227225C715981a30F3A44E"): "alm",
				common.HexToAddress("0x1369f7b2b38c76b6478c0f0e66d94923421891ba"): "subproxy",
			},
		},
	}

	for _, tc := range cases {
		got := byStarChain[[2]string{tc.star, tc.chain}]
		if got == nil {
			t.Fatalf("no proxies for %s/%s", tc.star, tc.chain)
		}
		for addr, wantRole := range tc.want {
			role, ok := got[addr]
			if !ok {
				t.Errorf("%s/%s missing proxy %s", tc.star, tc.chain, addr.Hex())
				continue
			}
			if role != wantRole {
				t.Errorf("%s/%s proxy %s role = %q, want %q", tc.star, tc.chain, addr.Hex(), role, wantRole)
			}
		}
	}
}
