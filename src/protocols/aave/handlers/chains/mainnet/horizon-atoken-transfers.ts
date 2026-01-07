import { ponder } from "ponder:registry";
// import * as schema from "@aave/schema/chains/mainnet";
// import { handleSpTokenTransfer } from "@sparklend/utils/transfer-tracker";

/**
 * Register Aave Mainnet Horizon Market AToken Transfer Handlers
 * 
 * TODO: Uncomment and implement once AaveMainnetHorizonAToken addresses are added to contracts.ts
 * This will enable portfolio snapshot tracking similar to Sparklend.
 */
export function registerAaveMainnetHorizonATokenTransferHandlers() {
  // const chainId = "mainnet";

  // TODO: Uncomment when AaveMainnetHorizonAToken is configured
  // ponder.on("AaveMainnetHorizonAToken:Transfer", async ({ event, context }) => {
  //   const blockNumber = BigInt(event.block.number);
  //   const timestamp = BigInt(event.block.timestamp);

  //   await handleSpTokenTransfer(
  //     context,
  //     chainId,
  //     schema.AaveMainnetHorizonUserScaledSupplyPosition,
  //     schema.AaveMainnetHorizonUserSupplyPosition,
  //     schema.AaveMainnetHorizonReserveDataUpdated,
  //     schema.AaveMainnetHorizonActiveUser,
  //     event.log.address as `0x${string}`,
  //     event.args.from,
  //     event.args.to,
  //     event.args.value,
  //     blockNumber,
  //     timestamp
  //   );
  // });
}

