package abis

import "testing"

func TestSyrupVaultEventsABI_ParsesAllEvents(t *testing.T) {
	a, err := GetSyrupVaultEventsABI()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Deposit", "Withdraw", "Transfer"} {
		if _, ok := a.Events[name]; !ok {
			t.Fatalf("missing event %s", name)
		}
	}
}

func TestSyrupVaultViewsABI_ParsesAllMethods(t *testing.T) {
	a, err := GetSyrupVaultViewsABI()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"totalAssets", "totalSupply", "convertToAssets", "balanceOf", "decimals", "asset", "name", "symbol"} {
		if _, ok := a.Methods[name]; !ok {
			t.Fatalf("missing method %s", name)
		}
	}
}
