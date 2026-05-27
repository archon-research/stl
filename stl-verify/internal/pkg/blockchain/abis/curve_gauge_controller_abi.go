package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetCurveGaugeControllerEventsABI returns the ABI for the canonical Curve
// GaugeController (mainnet `0x2F50D538606Fa9EDD2B11E2446BEb18C9D5846bB`).
//
// Both `KillGauge` and `Killed` shapes are included: different historical
// GaugeController implementations have used both names. Including both lets
// the decoder match whichever the on-chain contract actually emits without
// guessing.
func GetCurveGaugeControllerEventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "addr",       "type": "address"},
				{"indexed": false, "name": "gauge_type", "type": "int128"},
				{"indexed": false, "name": "weight",     "type": "uint256"}
			],
			"name": "NewGauge",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "addr", "type": "address"}
			],
			"name": "KillGauge",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "addr", "type": "address"}
			],
			"name": "Killed",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "addr", "type": "address"}
			],
			"name": "UnkillGauge",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "addr", "type": "address"}
			],
			"name": "Unkilled",
			"type": "event"
		}
	]`)
}

// GetCurveGaugeControllerReadABI returns the ABI for GaugeController view
// methods used during gauge bootstrap.
func GetCurveGaugeControllerReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [{"name": "", "type": "address"}],
			"name": "gauge_types",
			"outputs": [{"name": "", "type": "int128"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
