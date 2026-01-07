/**
 * Aave Mainnet Handlers
 * 
 * Registers all event handlers for Aave V3 Core and Horizon markets on Mainnet.
 */

import { registerAaveMainnetCoreHandlers } from "./core-pool";
import { registerAaveMainnetHorizonHandlers } from "./horizon-pool";
import { registerAaveMainnetCoreATokenTransferHandlers } from "./core-atoken-transfers";
import { registerAaveMainnetHorizonATokenTransferHandlers } from "./horizon-atoken-transfers";

// Register all handlers
registerAaveMainnetCoreHandlers();
registerAaveMainnetHorizonHandlers();

// AToken transfer handlers (for portfolio snapshots - currently TODO)
registerAaveMainnetCoreATokenTransferHandlers();
registerAaveMainnetHorizonATokenTransferHandlers();


