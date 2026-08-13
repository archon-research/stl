package abis

import "testing"

func TestGetCurveNGPoolABI(t *testing.T) {
	a, err := GetCurveNGPoolABI()
	if err != nil {
		t.Fatalf("GetCurveNGPoolABI: %v", err)
	}

	method, ok := a.Methods["get_virtual_price"]
	if !ok {
		t.Fatal("get_virtual_price method missing")
	}
	if len(method.Inputs) != 0 {
		t.Errorf("get_virtual_price inputs = %d, want 0", len(method.Inputs))
	}
	if len(method.Outputs) != 1 || method.Outputs[0].Type.String() != "uint256" {
		t.Errorf("get_virtual_price outputs = %v, want single uint256", method.Outputs)
	}

	if _, err := a.Pack("get_virtual_price"); err != nil {
		t.Errorf("packing get_virtual_price: %v", err)
	}

	if len(a.Methods) != 1 {
		t.Errorf("methods = %d, want 1 (ABI stays minimal: only the method oracle pricing uses)", len(a.Methods))
	}
}
