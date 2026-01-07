import { ponder } from "ponder:registry";
import * as schema from "@sparklend/schema/chains/mainnet";
import { handleSpTokenTransfer } from "@sparklend/utils/transfer-tracker";
import { CHAIN_IDS } from "@/constants";

/**
 * Register Sparklend Mainnet SpToken Transfer Handlers
 */
export function registerSparklendMainnetSpTokenTransferHandlers() {
  const CHAIN_IDENTIFIER = "mainnet"; // Lowercase string used in IDs
  const CHAIN_ID = CHAIN_IDS.mainnet; // Numeric chain ID (1)
  const PROTOCOL_ID = "sparklend-mainnet";

  ponder.on("SparklendMainnetSpToken:Transfer", async ({ event, context }) => {
    const blockNumber = BigInt(event.block.number);
    const timestamp = BigInt(event.block.timestamp);

    await handleSpTokenTransfer(
      context,
      CHAIN_IDENTIFIER,
      PROTOCOL_ID,
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

