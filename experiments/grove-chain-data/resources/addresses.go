package resources

type ExtraData struct {
	VaultAddress string
}

type TokenData struct {
	ContractAddress string
	WalletAddress   string
	AssetAddress    string
	Star            string
	Protocol        string
	AllocationType  string
	ExtraData       *ExtraData
	CreatedAtBlock  int
}

var GroveTokensData = map[string][]TokenData{
	"ethereum": {
		{
			ContractAddress: "0x5a0f93d040de44e78f251b03c43be9cf317dcf64",
			WalletAddress:   "0x491edfb0b8b608044e227225c715981a30f3a44e",
			AssetAddress:    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
			Star:            "grove",
			Protocol:        "centrifuge",
			AllocationType:  "allocation",
			ExtraData: &ExtraData{
				VaultAddress: "0x4880799eE5200fC58DA299e965df644fBf46780B",
			},
		},
		{
			ContractAddress: "0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041",
			WalletAddress:   "0x491edfb0b8b608044e227225c715981a30f3a44e",
			AssetAddress:    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
			Star:            "grove",
			Protocol:        "blackrock",
			AllocationType:  "allocation",
		},
		{
			ContractAddress: "0x6b175474e89094c44da98b954eedeac495271d0f",
			WalletAddress:   "0x491edfb0b8b608044e227225c715981a30f3a44e",
			Star:            "grove",
			Protocol:        "sky",
			AllocationType:  "asset",
		},
		{
			ContractAddress: "0x8c213ee79581ff4984583c6a801e5263418c4b86",
			WalletAddress:   "0x491edfb0b8b608044e227225c715981a30f3a44e",
			AssetAddress:    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
			Star:            "grove",
			Protocol:        "centrifuge",
			AllocationType:  "allocation",
			ExtraData: &ExtraData{
				VaultAddress: "0xFE6920eB6C421f1179cA8c8d4170530CDBdfd77A",
			},
		},
		{
			ContractAddress: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
			WalletAddress:   "0x491edfb0b8b608044e227225c715981a30f3a44e",
			Star:            "grove",
			AllocationType:  "asset",
		},
		{
			ContractAddress: "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
			WalletAddress:   "0x491edfb0b8b608044e227225c715981a30f3a44e",
			Star:            "grove",
			Protocol:        "sky",
			AllocationType:  "pol",
		},
		{
			ContractAddress: "0xfa82580c16a31d0c1bc632a36f82e83efef3eec0",
			WalletAddress:   "0x491edfb0b8b608044e227225c715981a30f3a44e",
			AssetAddress:    "0x8292bb45bf1ee4d140127049757c2e0ff06317ed",
			Star:            "grove",
			Protocol:        "aave",
			AllocationType:  "allocation",
		},
		{
			ContractAddress: "0xe3190143eb552456f88464662f0c0c4ac67a77eb",
			WalletAddress:   "0x491edfb0b8b608044e227225c715981a30f3a44e",
			AssetAddress:    "0x8292bb45bf1ee4d140127049757c2e0ff06317ed",
			Star:            "grove",
			Protocol:        "aave",
			AllocationType:  "allocation",
		},
		{
			ContractAddress: "0x68215b6533c47ff9f7125ac95adf00fe4a62f79e",
			WalletAddress:   "0x491edfb0b8b608044e227225c715981a30f3a44e",
			AssetAddress:    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
			Star:            "grove",
			Protocol:        "aave",
			AllocationType:  "allocation",
		},
		{
			ContractAddress: "0x8292bb45bf1ee4d140127049757c2e0ff06317ed",
			WalletAddress:   "0x491edfb0b8b608044e227225c715981a30f3a44e",
			Star:            "grove",
			Protocol:        "ripple",
			AllocationType:  "asset",
		},
	},
	"avalanche": {
		{
			ContractAddress: "0x58f93d6b1ef2f44ec379cb975657c132cbed3b6b",
			WalletAddress:   "0x7107dd8f56642327945294a18a4280c78e153644",
			AssetAddress:    "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
			Star:            "grove",
			Protocol:        "centrifuge",
			AllocationType:  "allocation",
			ExtraData: &ExtraData{
				VaultAddress: "0x1121F4e21eD8B9BC1BB9A2952cDD8639aC897784",
			},
		},
	},
	"plume": {
		{
			ContractAddress: "0x9477724bb54ad5417de8baff29e59df3fb4da74f",
			WalletAddress:   "0x1db91ad50446a671e2231f77e00948e68876f812",
			AssetAddress:    "0x222365ef19f7947e5484218551b56bb3965aa7af",
			Star:            "grove",
			Protocol:        "centrifuge",
			AllocationType:  "allocation",
			ExtraData: &ExtraData{
				VaultAddress: "0x354a9222571259457B2e98b2285B62e6a9bf4eD3",
			},
		},
		{
			ContractAddress: "0xa5d465251fbcc907f5dd6bb2145488dfc6a2627b",
			WalletAddress:   "0x1db91ad50446a671e2231f77e00948e68876f812",
			AssetAddress:    "0x222365ef19f7947e5484218551b56bb3965aa7af",
			Star:            "grove",
			Protocol:        "centrifuge",
			AllocationType:  "allocation",
			ExtraData: &ExtraData{
				VaultAddress: "0x354a9222571259457B2e98b2285B62e6a9bf4eD3",
			},
		},
		{
			ContractAddress: "0x222365ef19f7947e5484218551b56bb3965aa7af",
			WalletAddress:   "0x1db91ad50446a671e2231f77e00948e68876f812",
			Star:            "grove",
			AllocationType:  "asset",
		},
	},
	"monad": {
		{
			ContractAddress: "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
			WalletAddress:   "0x94b398acb2fce988871218221ea6a4a2b26cccbc",
			Star:            "grove",
			Protocol:        "agora",
			AllocationType:  "asset",
			CreatedAtBlock:  36870131,
		},
		{
			ContractAddress: "0x6b405dca74897c9442d369dcf6c0ec230f7e1c7c",
			WalletAddress:   "0x94b398acb2fce988871218221ea6a4a2b26cccbc",
			AssetAddress:    "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
			Star:            "grove",
			Protocol:        "uniswap",
			AllocationType:  "allocation",
			CreatedAtBlock:  39598901,
		},
	},
}
