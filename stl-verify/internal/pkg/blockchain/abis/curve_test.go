package abis

import "testing"

func TestCurveStableswapABI_HasTokenExchange(t *testing.T) {
	a, err := CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	if _, ok := a.Events["TokenExchange"]; !ok {
		t.Fatal("TokenExchange event missing")
	}
	if _, ok := a.Methods["get_virtual_price"]; !ok {
		t.Fatal("get_virtual_price method missing")
	}
}

func TestCurveStableswapABI_HasAllRequiredEntries(t *testing.T) {
	a, err := CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}

	requiredEvents := []string{
		"TokenExchange",
		"AddLiquidity",
		"RemoveLiquidity",
		"RemoveLiquidityOne",
		"RemoveLiquidityImbalance",
		"RampA",
		"StopRampA",
	}
	for _, name := range requiredEvents {
		if _, ok := a.Events[name]; !ok {
			t.Errorf("event %q missing", name)
		}
	}

	requiredMethods := []string{
		"coins",
		"balances",
		"get_virtual_price",
		"A",
		"fee",
		"get_dy",
		"totalSupply",
		"price_oracle",
		"last_price",
		"stored_rates",
	}
	for _, name := range requiredMethods {
		if _, ok := a.Methods[name]; !ok {
			t.Errorf("method %q missing", name)
		}
	}
}

func TestCurveCryptoswapABI_HasGamma(t *testing.T) {
	a, err := CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	if _, ok := a.Methods["gamma"]; !ok {
		t.Fatal("gamma method missing")
	}
}

func TestCurveCryptoswapABI_HasAllRequiredEntries(t *testing.T) {
	a, err := CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}

	requiredEvents := []string{
		"TokenExchange",
		"AddLiquidity",
		"RemoveLiquidity",
		"RemoveLiquidityOne",
	}
	for _, name := range requiredEvents {
		if _, ok := a.Events[name]; !ok {
			t.Errorf("event %q missing", name)
		}
	}

	requiredMethods := []string{
		"coins",
		"balances",
		"get_virtual_price",
		"A",
		"gamma",
		"fee",
		"get_dy",
		"totalSupply",
		"price_scale",
		"price_oracle",
		"last_prices",
		"D",
		"xcp_profit",
	}
	for _, name := range requiredMethods {
		if _, ok := a.Methods[name]; !ok {
			t.Errorf("method %q missing", name)
		}
	}
}

func TestCurveStableswapABI_TokenExchangeFields(t *testing.T) {
	a, err := CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	ev, ok := a.Events["TokenExchange"]
	if !ok {
		t.Fatal("TokenExchange event missing")
	}
	// buyer (indexed) + sold_id (int128) + tokens_sold (uint256) + bought_id (int128) + tokens_bought (uint256) = 5 inputs
	if len(ev.Inputs) != 5 {
		t.Errorf("TokenExchange: want 5 inputs, got %d", len(ev.Inputs))
	}
}

func TestCurveCryptoswapABI_TokenExchangeFields(t *testing.T) {
	a, err := CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	ev, ok := a.Events["TokenExchange"]
	if !ok {
		t.Fatal("TokenExchange event missing")
	}
	// buyer (indexed) + sold_id + tokens_sold + bought_id + tokens_bought + fee + packed_price_scale = 7 inputs
	if len(ev.Inputs) != 7 {
		t.Errorf("TokenExchange: want 7 inputs, got %d", len(ev.Inputs))
	}
}

func TestCurveStableswapABI_RemoveLiquidityOneIsNGForm(t *testing.T) {
	a, err := CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	ev, ok := a.Events["RemoveLiquidityOne"]
	if !ok {
		t.Fatal("RemoveLiquidityOne event missing")
	}
	// NG form: provider (indexed) + token_id (int128) + token_amount + coin_amount + token_supply = 5 inputs
	if len(ev.Inputs) != 5 {
		t.Errorf("RemoveLiquidityOne: want 5 inputs (NG form), got %d", len(ev.Inputs))
	}
	// Verify token_id is the second input and is int128.
	if ev.Inputs[1].Name != "token_id" {
		t.Errorf("RemoveLiquidityOne input[1]: want name token_id, got %q", ev.Inputs[1].Name)
	}
	if ev.Inputs[1].Type.String() != "int128" {
		t.Errorf("RemoveLiquidityOne input[1]: want type int128, got %q", ev.Inputs[1].Type.String())
	}
}

func TestCurveStableswapABI_NGOracleMethodsAreNoArg(t *testing.T) {
	a, err := CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	for _, name := range []string{"price_oracle", "last_price"} {
		m, ok := a.Methods[name]
		if !ok {
			t.Errorf("method %q missing", name)
			continue
		}
		if len(m.Inputs) != 0 {
			t.Errorf("method %q: want 0 inputs (no-arg), got %d", name, len(m.Inputs))
		}
	}
}
