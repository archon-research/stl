import { ponder } from "ponder:registry";
import * as schema from "@sparklend/schema/chains/mainnet";
import { handleSpTokenTransfer } from "@sparklend/utils/transfer-tracker";

/**
 * Register Sparklend Mainnet SpToken Transfer Handlers
 */
export function registerSparklendMainnetSpTokenTransferHandlers() {
  const chainId = "mainnet";
  const chainName = "Mainnet";

  ponder.on("SparklendMainnetSpToken:Transfer", async ({ event, context }) => {
    const blockNumber = BigInt(event.block.number);
    const timestamp = BigInt(event.block.timestamp);

    await handleSpTokenTransfer(
      context,
      chainId,
      chainName,
      schema.SparklendMainnetUserScaledSupplyPosition,
      schema.SparklendMainnetUserSupplyPosition,
      schema.SparklendMainnetReserveDataUpdated,
      schema.SparklendMainnetActiveUser,
      event.log.address as `0x${string}`,
      event.args.from,
      event.args.to,
      event.args.value,
      blockNumber,
      timestamp
    );
  });
}

