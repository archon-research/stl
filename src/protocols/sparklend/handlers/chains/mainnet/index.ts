/**
 * Sparklend Mainnet Handlers
 *
 * Registers all handler functions for Mainnet
 */

import { registerSparklendMainnetPoolEventHandlers } from "./pool-events";
import { registerSparklendMainnetSpTokenTransferHandlers } from "./sptoken-transfers";

// Register all handlers
registerSparklendMainnetPoolEventHandlers();
registerSparklendMainnetSpTokenTransferHandlers();

