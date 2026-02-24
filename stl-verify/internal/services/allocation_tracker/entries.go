package allocation_tracker

import "github.com/ethereum/go-ethereum/common"

func addr(hex string) *common.Address {
	a := common.HexToAddress(hex)
	return &a
}

func blockNum(n int64) *int64 { return &n }

func DefaultTokenEntries() []*TokenEntry {
	var entries []*TokenEntry
	entries = append(entries, mainnetEntries()...)
	entries = append(entries, baseEntries()...)
	entries = append(entries, arbitrumEntries()...)
	entries = append(entries, optimismEntries()...)
	entries = append(entries, unichainEntries()...)
	entries = append(entries, avalancheEntries()...)
	entries = append(entries, plumeEntries()...)
	entries = append(entries, monadEntries()...)
	return entries
}

func BuildEntryLookup(entries []*TokenEntry) map[EntryKey]*TokenEntry {
	m := make(map[EntryKey]*TokenEntry, len(entries))
	for _, e := range entries {
		m[e.Key()] = e
	}
	return m
}

func EntriesForChain(entries []*TokenEntry, chain string) []*TokenEntry {
	var result []*TokenEntry
	for _, e := range entries {
		if e.Chain == chain {
			result = append(result, e)
		}
	}
	return result
}

// ===================== MAINNET =====================

func mainnetEntries() []*TokenEntry {
	spark := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	grove := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")

	usdc := addr("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
	dai := addr("0x6b175474e89094c44da98b954eedeac495271d0f")
	usds := addr("0xdc035d45d973e3ec169d2276ddab16f1e407384f")
	usdt := addr("0xdac17f958d2ee523a2206206994597c13d831ec7")
	pyusd := addr("0x6c3ea9036406852006290770bedfcaba0e23a0e8")
	susds := addr("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd")
	usde := addr("0x4c9edd5852cd905f086c759e8383e09bff1e68b3")
	rlusd := addr("0x8292bb45bf1ee4d140127049757c2e0ff06317ed")
	ausd := addr("0x00000000efe302beaa2b3e6e1b18d08d69a9012a")

	return []*TokenEntry{
		// ── Spark: SparkLend atokens (skip — existing worker) ──
		{ContractAddress: common.HexToAddress("0x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b"), WalletAddress: spark, AssetAddress: dai, Star: "spark", Chain: "mainnet", Protocol: "sparklend", AllocationType: "allocation", TokenType: "atoken"},
		{ContractAddress: common.HexToAddress("0x377c3bd93f2a2984e1e7be6a5c22c525ed4a4815"), WalletAddress: spark, AssetAddress: usdc, Star: "spark", Chain: "mainnet", Protocol: "sparklend", AllocationType: "allocation", TokenType: "atoken"},
		{ContractAddress: common.HexToAddress("0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359"), WalletAddress: spark, AssetAddress: usds, Star: "spark", Chain: "mainnet", Protocol: "sparklend", AllocationType: "allocation", TokenType: "atoken"},
		{ContractAddress: common.HexToAddress("0xe7df13b8e3d6740fe17cbe928c7334243d86c92f"), WalletAddress: spark, AssetAddress: usdt, Star: "spark", Chain: "mainnet", Protocol: "sparklend", AllocationType: "allocation", TokenType: "atoken"},
		{ContractAddress: common.HexToAddress("0x779224df1c756b4edd899854f32a53e8c2b2ce5d"), WalletAddress: spark, AssetAddress: pyusd, Star: "spark", Chain: "mainnet", Protocol: "sparklend", AllocationType: "allocation", TokenType: "atoken", CreatedAtBlock: blockNum(23118264)},

		// ── Spark: Aave atokens (skip — existing worker) ──
		{ContractAddress: common.HexToAddress("0x09aa30b182488f769a9824f15e6ce58591da4781"), WalletAddress: spark, AssetAddress: usds, Star: "spark", Chain: "mainnet", Protocol: "aave", AllocationType: "allocation", TokenType: "atoken"},
		{ContractAddress: common.HexToAddress("0x32a6268f9ba3642dda7892add74f1d34469a4259"), WalletAddress: spark, AssetAddress: usds, Star: "spark", Chain: "mainnet", Protocol: "aave", AllocationType: "allocation", TokenType: "atoken"},
		{ContractAddress: common.HexToAddress("0x98c23e9d8f34fefb1b7bd6a91b7ff122f4e16f5c"), WalletAddress: spark, AssetAddress: usdc, Star: "spark", Chain: "mainnet", Protocol: "aave", AllocationType: "allocation", TokenType: "atoken"},

		// ── Spark: Morpho vaults (ERC4626) ──
		{ContractAddress: common.HexToAddress("0x56a76b428244a50513ec81e225a293d128fd581d"), WalletAddress: spark, AssetAddress: usdc, Star: "spark", Chain: "mainnet", Protocol: "morpho", AllocationType: "allocation", TokenType: "erc4626", CreatedAtBlock: blockNum(23319630)},
		{ContractAddress: common.HexToAddress("0x73e65dbd630f90604062f6e02fab9138e713edd9"), WalletAddress: spark, AssetAddress: dai, Star: "spark", Chain: "mainnet", Protocol: "morpho", AllocationType: "allocation", TokenType: "erc4626"},
		{ContractAddress: common.HexToAddress("0xe41a0583334f0dc4e023acd0bfef3667f6fe0597"), WalletAddress: spark, AssetAddress: usds, Star: "spark", Chain: "mainnet", Protocol: "morpho", AllocationType: "allocation", TokenType: "erc4626", CreatedAtBlock: blockNum(22932160)},

		// ── Spark: Maple (ERC4626) ──
		{ContractAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"), WalletAddress: spark, AssetAddress: usdc, Star: "spark", Chain: "mainnet", Protocol: "maple", AllocationType: "allocation", TokenType: "erc4626"},
		{ContractAddress: common.HexToAddress("0x356B8d89c1e1239Cbbb9dE4815c39A1474d5BA7D"), WalletAddress: spark, AssetAddress: usdt, Star: "spark", Chain: "mainnet", Protocol: "maple", AllocationType: "allocation", TokenType: "erc4626"},

		// ── Spark: Fluid (ERC4626) ──
		{ContractAddress: common.HexToAddress("0x2bbe31d63e6813e3ac858c04dae43fb2a72b0d11"), WalletAddress: spark, AssetAddress: susds, Star: "spark", Chain: "mainnet", Protocol: "fluid", AllocationType: "allocation", TokenType: "erc4626"},

		// ── Spark: Arkis (ERC4626) ──
		{ContractAddress: common.HexToAddress("0x38464507e02c983f20428a6e8566693fe9e422a9"), WalletAddress: spark, AssetAddress: usdc, Star: "spark", Chain: "mainnet", Protocol: "arkis", AllocationType: "allocation", TokenType: "erc4626", CreatedAtBlock: blockNum(23896879)},

		// ── Spark: sUSDS (ERC4626) ──
		{ContractAddress: common.HexToAddress("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd"), WalletAddress: spark, AssetAddress: usds, Star: "spark", Chain: "mainnet", Protocol: "sky", AllocationType: "pol", TokenType: "erc4626"},

		// ── Spark: Ethena ──
		{ContractAddress: common.HexToAddress("0x4c9edd5852cd905f086c759e8383e09bff1e68b3"), WalletAddress: spark, Star: "spark", Chain: "mainnet", Protocol: "ethena", AllocationType: "allocation", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0x9d39a5de30e57443bff2a8307a4256c8797a3497"), WalletAddress: spark, AssetAddress: usde, Star: "spark", Chain: "mainnet", Protocol: "ethena", AllocationType: "allocation", TokenType: "erc4626"},

		// ── Spark: Curve ──
		{ContractAddress: common.HexToAddress("0x00836fe54625be242bcfa286207795405ca4fd10"), WalletAddress: spark, AssetAddress: usdt, Star: "spark", Chain: "mainnet", Protocol: "curve", AllocationType: "allocation", TokenType: "curve"},
		{ContractAddress: common.HexToAddress("0xa632d59b9b804a956bfaa9b48af3a1b74808fc1f"), WalletAddress: spark, AssetAddress: pyusd, Star: "spark", Chain: "mainnet", Protocol: "curve", AllocationType: "allocation", TokenType: "curve", CreatedAtBlock: blockNum(23390887)},

		// ── Spark: Superstate ──
		{ContractAddress: common.HexToAddress("0x43415eb6ff9db7e26a15b704e7a3edce97d31c4e"), WalletAddress: spark, AssetAddress: usdc, Star: "spark", Chain: "mainnet", Protocol: "superstate", AllocationType: "allocation", TokenType: "superstate"},
		{ContractAddress: common.HexToAddress("0x14d60e7fdc0d71d8611742720e4c50e7a974020c"), WalletAddress: spark, AssetAddress: usdc, Star: "spark", Chain: "mainnet", Protocol: "superstate", AllocationType: "allocation", TokenType: "superstate"},

		// ── Spark: BlackRock BUIDL ──
		{ContractAddress: common.HexToAddress("0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041"), WalletAddress: spark, AssetAddress: usdc, Star: "spark", Chain: "mainnet", Protocol: "blackrock", AllocationType: "allocation", TokenType: "buidl"},

		// ── Spark: Janus Henderson ──
		{ContractAddress: common.HexToAddress("0x8c213ee79581ff4984583c6a801e5263418c4b86"), WalletAddress: spark, AssetAddress: usdc, Star: "spark", Chain: "mainnet", Protocol: "janus-henderson", AllocationType: "allocation", TokenType: "erc20"},

		// ── Spark: Anchorage (off-chain) ──
		{ContractAddress: common.HexToAddress("0x49506c3aa028693458d6ee816b2ec28522946872"), WalletAddress: spark, Star: "spark", Chain: "mainnet", Protocol: "anchorage", AllocationType: "allocation", TokenType: "anchorage", CreatedAtBlock: blockNum(24019368)},

		// ── Spark: ERC20 assets ──
		{ContractAddress: common.HexToAddress("0x6b175474e89094c44da98b954eedeac495271d0f"), WalletAddress: spark, Star: "spark", Chain: "mainnet", Protocol: "sky", AllocationType: "asset", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"), WalletAddress: spark, Star: "spark", Chain: "mainnet", AllocationType: "asset", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7"), WalletAddress: spark, Star: "spark", Chain: "mainnet", AllocationType: "asset", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0xdc035d45d973e3ec169d2276ddab16f1e407384f"), WalletAddress: spark, Star: "spark", Chain: "mainnet", Protocol: "sky", AllocationType: "pol", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0x6c3ea9036406852006290770bedfcaba0e23a0e8"), WalletAddress: spark, Star: "spark", Chain: "mainnet", Protocol: "paypal", AllocationType: "asset", TokenType: "erc20", CreatedAtBlock: blockNum(23118264)},

		// ── Grove: Aave atokens (skip — existing worker) ──
		{ContractAddress: common.HexToAddress("0xfa82580c16a31d0c1bc632a36f82e83efef3eec0"), WalletAddress: grove, AssetAddress: rlusd, Star: "grove", Chain: "mainnet", Protocol: "aave", AllocationType: "allocation", TokenType: "atoken", CreatedAtBlock: blockNum(22319334)},
		{ContractAddress: common.HexToAddress("0xe3190143eb552456f88464662f0c0c4ac67a77eb"), WalletAddress: grove, AssetAddress: rlusd, Star: "grove", Chain: "mainnet", Protocol: "aave", AllocationType: "allocation", TokenType: "atoken", CreatedAtBlock: blockNum(23132230)},
		{ContractAddress: common.HexToAddress("0x68215b6533c47ff9f7125ac95adf00fe4a62f79e"), WalletAddress: grove, AssetAddress: usdc, Star: "grove", Chain: "mainnet", Protocol: "aave", AllocationType: "allocation", TokenType: "atoken", CreatedAtBlock: blockNum(23132230)},

		// ── Grove: Janus Henderson ──
		{ContractAddress: common.HexToAddress("0x5a0f93d040de44e78f251b03c43be9cf317dcf64"), WalletAddress: grove, AssetAddress: usdc, Star: "grove", Chain: "mainnet", Protocol: "janus-henderson", AllocationType: "allocation", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0x8c213ee79581ff4984583c6a801e5263418c4b86"), WalletAddress: grove, AssetAddress: usdc, Star: "grove", Chain: "mainnet", Protocol: "janus-henderson", AllocationType: "allocation", TokenType: "erc20"},

		// ── Grove: BlackRock BUIDL ──
		{ContractAddress: common.HexToAddress("0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041"), WalletAddress: grove, AssetAddress: usdc, Star: "grove", Chain: "mainnet", Protocol: "blackrock", AllocationType: "allocation", TokenType: "buidl"},

		// ── Grove: Securitize STAC ──
		{ContractAddress: common.HexToAddress("0x51c2d74017390cbbd30550179a16a1c28f7210fc"), WalletAddress: grove, AssetAddress: usdc, Star: "grove", Chain: "mainnet", Protocol: "securitize", AllocationType: "allocation", TokenType: "securitize", CreatedAtBlock: blockNum(24041058)},

		// ── Grove: Steakhouse (ERC4626) ──
		{ContractAddress: common.HexToAddress("0xbeef2b5fd3d94469b7782aebe6364e6e6fb1b709"), WalletAddress: grove, AssetAddress: usdc, Star: "grove", Chain: "mainnet", Protocol: "steakhouse", AllocationType: "allocation", TokenType: "erc4626", CreatedAtBlock: blockNum(24148141)},
		{ContractAddress: common.HexToAddress("0xBEEfF0d672ab7F5018dFB614c93981045D4aA98a"), WalletAddress: grove, AssetAddress: ausd, Star: "grove", Chain: "mainnet", Protocol: "steakhouse", AllocationType: "allocation", TokenType: "erc4626"},

		// ── Grove: Uniswap V3 ──
		{ContractAddress: common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d"), WalletAddress: grove, AssetAddress: usdc, Star: "grove", Chain: "mainnet", Protocol: "uniswap", AllocationType: "allocation", TokenType: "uni_v3_pool"},

		// ── Grove: ERC20 assets ──
		{ContractAddress: common.HexToAddress("0x6b175474e89094c44da98b954eedeac495271d0f"), WalletAddress: grove, Star: "grove", Chain: "mainnet", Protocol: "sky", AllocationType: "asset", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"), WalletAddress: grove, Star: "grove", Chain: "mainnet", AllocationType: "asset", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0xdc035d45d973e3ec169d2276ddab16f1e407384f"), WalletAddress: grove, Star: "grove", Chain: "mainnet", Protocol: "sky", AllocationType: "pol", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0x8292bb45bf1ee4d140127049757c2e0ff06317ed"), WalletAddress: grove, Star: "grove", Chain: "mainnet", Protocol: "ripple", AllocationType: "asset", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0x00000000efe302beaa2b3e6e1b18d08d69a9012a"), WalletAddress: grove, Star: "grove", Chain: "mainnet", Protocol: "agora", AllocationType: "asset", TokenType: "erc20"},
	}
}

// ===================== BASE =====================

func baseEntries() []*TokenEntry {
	spark := common.HexToAddress("0x2917956eff0b5eaf030abdb4ef4296df775009ca")

	return []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e"), WalletAddress: spark, AssetAddress: addr("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"), Star: "spark", Chain: "base", Protocol: "psm3", AllocationType: "psm3", TokenType: "psm3"},
		{ContractAddress: common.HexToAddress("0x4e65fe4dba92790696d040ac24aa414708f5c0ab"), WalletAddress: spark, AssetAddress: addr("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"), Star: "spark", Chain: "base", Protocol: "aave", AllocationType: "allocation", TokenType: "atoken"},
		{ContractAddress: common.HexToAddress("0x5875eee11cf8398102fdad704c9e96607675467a"), WalletAddress: spark, AssetAddress: addr("0x820c137fa70c8691f0e44dc420a5e53c168921dc"), Star: "spark", Chain: "base", Protocol: "sky", AllocationType: "pol", TokenType: "proxy"},
		{ContractAddress: common.HexToAddress("0x7bfa7c4f149e7415b73bdedfe609237e29cbf34a"), WalletAddress: spark, AssetAddress: addr("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"), Star: "spark", Chain: "base", Protocol: "morpho", AllocationType: "allocation", TokenType: "erc4626"},
		{ContractAddress: common.HexToAddress("0xf62e339f21d8018940f188f6987bcdf02a849619"), WalletAddress: spark, AssetAddress: addr("0x5875eee11cf8398102fdad704c9e96607675467a"), Star: "spark", Chain: "base", Protocol: "fluid", AllocationType: "allocation", TokenType: "erc4626"},
		{ContractAddress: common.HexToAddress("0x820c137fa70c8691f0e44dc420a5e53c168921dc"), WalletAddress: spark, Star: "spark", Chain: "base", Protocol: "sky", AllocationType: "pol", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"), WalletAddress: spark, Star: "spark", Chain: "base", AllocationType: "asset", TokenType: "erc20"},
	}
}

// ===================== ARBITRUM =====================

func arbitrumEntries() []*TokenEntry {
	spark := common.HexToAddress("0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709")

	return []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x2b05f8e1cacc6974fd79a673a341fe1f58d27266"), WalletAddress: spark, AssetAddress: addr("0xaf88d065e77c8cc2239327c5edb3a432268e5831"), Star: "spark", Chain: "arbitrum", Protocol: "psm3", AllocationType: "psm3", TokenType: "psm3"},
		{ContractAddress: common.HexToAddress("0x3459fcc94390c3372c0f7b4cd3f8795f0e5afe96"), WalletAddress: spark, AssetAddress: addr("0xddb46999f8891663a8f2828d25298f70416d7610"), Star: "spark", Chain: "arbitrum", Protocol: "fluid", AllocationType: "allocation", TokenType: "erc4626"},
		{ContractAddress: common.HexToAddress("0x724dc807b04555b71ed48a6896b6f41593b8c637"), WalletAddress: spark, AssetAddress: addr("0xaf88d065e77c8cc2239327c5edb3a432268e5831"), Star: "spark", Chain: "arbitrum", Protocol: "aave", AllocationType: "allocation", TokenType: "atoken"},
		{ContractAddress: common.HexToAddress("0xddb46999f8891663a8f2828d25298f70416d7610"), WalletAddress: spark, AssetAddress: addr("0x6491c05a82219b8d1479057361ff1654749b876b"), Star: "spark", Chain: "arbitrum", Protocol: "sky", AllocationType: "pol", TokenType: "proxy"},
		{ContractAddress: common.HexToAddress("0x6491c05a82219b8d1479057361ff1654749b876b"), WalletAddress: spark, Star: "spark", Chain: "arbitrum", Protocol: "sky", AllocationType: "pol", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0xaf88d065e77c8cc2239327c5edb3a432268e5831"), WalletAddress: spark, Star: "spark", Chain: "arbitrum", AllocationType: "asset", TokenType: "erc20"},
	}
}

// ===================== OPTIMISM =====================

func optimismEntries() []*TokenEntry {
	spark := common.HexToAddress("0x876664f0c9ff24d1aa355ce9f1680ae1a5bf36fb")

	return []*TokenEntry{
		{ContractAddress: common.HexToAddress("0xe0f9978b907853f354d79188a3defbd41978af62"), WalletAddress: spark, AssetAddress: addr("0x0b2c639c533813f4aa9d7837caf62653d097ff85"), Star: "spark", Chain: "optimism", Protocol: "psm3", AllocationType: "psm3", TokenType: "psm3"},
		{ContractAddress: common.HexToAddress("0xb5b2dc7fd34c249f4be7fb1fcea07950784229e0"), WalletAddress: spark, AssetAddress: addr("0x4f13a96ec5c4cf34e442b46bbd98a0791f20edc3"), Star: "spark", Chain: "optimism", Protocol: "sky", AllocationType: "pol", TokenType: "proxy"},
		{ContractAddress: common.HexToAddress("0x0b2c639c533813f4aa9d7837caf62653d097ff85"), WalletAddress: spark, Star: "spark", Chain: "optimism", AllocationType: "asset", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0x4f13a96ec5c4cf34e442b46bbd98a0791f20edc3"), WalletAddress: spark, Star: "spark", Chain: "optimism", Protocol: "sky", AllocationType: "pol", TokenType: "erc20"},
	}
}

// ===================== UNICHAIN =====================

func unichainEntries() []*TokenEntry {
	spark := common.HexToAddress("0x345e368fccd62266b3f5f37c9a131fd1c39f5869")

	return []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x7b42ed932f26509465f7ce3faf76ffce1275312f"), WalletAddress: spark, AssetAddress: addr("0x078d782b760474a361dda0af3839290b0ef57ad6"), Star: "spark", Chain: "unichain", Protocol: "psm3", AllocationType: "psm3", TokenType: "psm3"},
		{ContractAddress: common.HexToAddress("0xa06b10db9f390990364a3984c04fadf1c13691b5"), WalletAddress: spark, AssetAddress: addr("0x7e10036acc4b56d4dfca3b77810356ce52313f9c"), Star: "spark", Chain: "unichain", Protocol: "sky", AllocationType: "pol", TokenType: "proxy"},
		{ContractAddress: common.HexToAddress("0x078d782b760474a361dda0af3839290b0ef57ad6"), WalletAddress: spark, Star: "spark", Chain: "unichain", AllocationType: "asset", TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0x7e10036acc4b56d4dfca3b77810356ce52313f9c"), WalletAddress: spark, Star: "spark", Chain: "unichain", Protocol: "sky", AllocationType: "pol", TokenType: "erc20"},
	}
}

// ===================== AVALANCHE-C =====================

func avalancheEntries() []*TokenEntry {
	spark := common.HexToAddress("0xece6b0e8a54c2f44e066fbb9234e7157b15b7fec")
	grove := common.HexToAddress("0x7107dd8f56642327945294a18a4280c78e153644")

	return []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x28b3a8fb53b741a8fd78c0fb9a6b2393d896a43d"), WalletAddress: spark, AssetAddress: addr("0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"), Star: "spark", Chain: "avalanche-c", Protocol: "spark", AllocationType: "allocation", TokenType: "erc4626", CreatedAtBlock: blockNum(69983672)},
		{ContractAddress: common.HexToAddress("0x58f93d6b1ef2f44ec379cb975657c132cbed3b6b"), WalletAddress: grove, AssetAddress: addr("0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"), Star: "grove", Chain: "avalanche-c", Protocol: "centrifuge", AllocationType: "allocation", TokenType: "centrifuge"},
		{ContractAddress: common.HexToAddress("0x2c0adff8e114f3ca106051144353ac703d24b901"), WalletAddress: grove, AssetAddress: addr("0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"), Star: "grove", Chain: "avalanche-c", Protocol: "arch", AllocationType: "allocation", TokenType: "galaxy_clo", CreatedAtBlock: blockNum(72633046)},
	}
}

// ===================== PLUME =====================

func plumeEntries() []*TokenEntry {
	grove := common.HexToAddress("0x1db91ad50446a671e2231f77e00948e68876f812")

	return []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x9477724bb54ad5417de8baff29e59df3fb4da74f"), WalletAddress: grove, AssetAddress: addr("0x222365ef19f7947e5484218551b56bb3965aa7af"), Star: "grove", Chain: "plume", Protocol: "centrifuge", AllocationType: "allocation", TokenType: "centrifuge_feeder"},
		{ContractAddress: common.HexToAddress("0xa5d465251fbcc907f5dd6bb2145488dfc6a2627b"), WalletAddress: grove, AssetAddress: addr("0x222365ef19f7947e5484218551b56bb3965aa7af"), Star: "grove", Chain: "plume", Protocol: "centrifuge", AllocationType: "allocation", TokenType: "centrifuge_feeder", CreatedAtBlock: blockNum(41311106)},
		{ContractAddress: common.HexToAddress("0x222365ef19f7947e5484218551b56bb3965aa7af"), WalletAddress: grove, Star: "grove", Chain: "plume", AllocationType: "asset", TokenType: "erc20"},
	}
}

// ===================== MONAD =====================

func monadEntries() []*TokenEntry {
	grove := common.HexToAddress("0x94b398acb2fce988871218221ea6a4a2b26cccbc")

	return []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x00000000efe302beaa2b3e6e1b18d08d69a9012a"), WalletAddress: grove, Star: "grove", Chain: "monad", Protocol: "agora", AllocationType: "asset", TokenType: "erc20", CreatedAtBlock: blockNum(36870131)},
		{ContractAddress: common.HexToAddress("0x6b405dca74897c9442d369dcf6c0ec230f7e1c7c"), WalletAddress: grove, AssetAddress: addr("0x00000000efe302beaa2b3e6e1b18d08d69a9012a"), Star: "grove", Chain: "monad", Protocol: "uniswap", AllocationType: "allocation", TokenType: "uni_v3_lp", CreatedAtBlock: blockNum(39598901)},
	}
}
