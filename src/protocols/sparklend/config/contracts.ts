import { AaveV3PoolAbi } from "@sparklend/abis/AaveV3PoolAbi";
import { ERC20Abi } from "@sparklend/abis/ERC20Abi";
import { SPARKLEND_MAINNET_POOL_ADDRESS, SPARKLEND_GNOSIS_POOL_ADDRESS } from "@/constants";

// Mainnet Contracts
const mainnetContracts = {
  SparklendMainnetPool: {
    chain: "mainnet" as const,
    abi: AaveV3PoolAbi,
    address: SPARKLEND_MAINNET_POOL_ADDRESS,
    startBlock: 16776401
  },
  SparklendMainnetSpToken: {
    chain: "mainnet" as const,
    abi: ERC20Abi,
    address: [
      "0x59cD1C87501baa753d0B5B5Ab5D8416A45cD71DB", // spWETH
      "0x12B54025C112Aa61fAce2CDB7118740875A566E9", // spwstETH
      "0x78f897F0fE2d3B5690EbAe7f19862DEacedF10a7", // spsDAI
      "0x377C3bd93f2a2984E1E7bE6A5C22c525eD4A4815", // spUSDC
      "0xe7dF13b8e3d6740fe17CBE928C7334243d86c92f", // spUSDT
      "0x9985dF20D7e9103ECBCeb16a84956434B6f06ae8", // sprETH
      "0x7b481aCC9fDADDc9af2cBEA1Ff2342CB1733E50F", // spGNO
      "0x4197ba364AE6698015AE5c1468f54087602715b2", // spWBTC
    ] as const,
    startBlock: 16776401
  },
};

// Gnosis Chain Contracts
const gnosisContracts = {
  SparklendGnosisPool: {
    chain: "gnosis" as const,
    abi: AaveV3PoolAbi,
    address: SPARKLEND_GNOSIS_POOL_ADDRESS,
    startBlock: 29817457,
  },
  // TODO: Add spToken addresses for Gnosis once deployed/discovered
  // SparklendGnosisSpToken: {
  //   chain: "gnosis" as const,
  //   abi: ERC20Abi,
  //   address: [] as const,
  //   startBlock: 29817457,
  // },
};

export const sparklendContracts = {
  ...mainnetContracts,
  ...gnosisContracts,
};
