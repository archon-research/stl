/**
 * Aave Protocol Contract Configuration
 * 
 * Defines contracts for both Aave V3 Core and Horizon markets across multiple chains.
 */

import { AaveV3CorePoolAbi } from "../abis/AaveV3CorePoolAbi";
import { AaveV3HorizonPoolAbi } from "../abis/AaveV3HorizonPoolAbi";
import { ERC20Abi } from "../abis/ERC20Abi";
import { AAVE_V3_CORE_POOL_ADDRESS, AAVE_V3_HORIZON_POOL_ADDRESS } from "@/constants";

// Mainnet Contracts
const mainnetContracts = {
  // Aave V3 Core Market
  AaveMainnetCorePool: {
    chain: "mainnet" as const,
    abi: AaveV3CorePoolAbi,
    address: AAVE_V3_CORE_POOL_ADDRESS,
    startBlock: 16291127,
  },
  
  // TODO: Add Core Market aToken addresses for transfer tracking (needed for portfolio snapshots)
  // Discovery via ReserveInitialized events or Aave V3 subgraph
  // AaveMainnetCoreAToken: {
  //   chain: "mainnet" as const,
  //   abi: ERC20Abi,
  //   address: [] as const,
  //   startBlock: 16291127,
  // },
  
  // Aave V3 Horizon Market
  AaveMainnetHorizonPool: {
    chain: "mainnet" as const,
    abi: AaveV3HorizonPoolAbi,
    address: AAVE_V3_HORIZON_POOL_ADDRESS,
    startBlock: 23125535,
  },

  // TODO: Add Horizon Market aToken addresses for transfer tracking (needed for portfolio snapshots)
  // Discovery via ReserveInitialized events or Aave V3 subgraph
  // AaveMainnetHorizonAToken: {
  //   chain: "mainnet" as const,
  //   abi: ERC20Abi,
  //   address: [] as const,
  //   startBlock: 23125535,
  // },
} as const;

// Export all Aave contracts
export const aaveContracts = {
  ...mainnetContracts,
  // Future chains can be added here:
  // ...gnosisContracts,
  // ...arbitrumContracts,
} as const;

