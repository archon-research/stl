/**
 * Sparklend Protocol Module
 *
 * Main entry point for Sparklend protocol indexing across all chains.
 * Exports all configuration, schemas, and handlers.
 */

// Configuration
export * from "@sparklend/config";

// Schemas
export * as sparklendSchema from "@sparklend/schema";

// Handlers
export * from "@sparklend/handlers";

// Utilities (for external use if needed)
export * as sparklendUtils from "@sparklend/utils";

/**
 * Initialize and register all Sparklend handlers for all chains
 */
export function initializeSparklendProtocol() {
  const {
    // Mainnet handlers
    registerSparklendMainnetPoolEventHandlers,
    registerSparklendMainnetSpTokenTransferHandlers,
    registerSparklendMainnetPriceCaptureHandler,
    registerSparklendMainnetSnapshotCalculationHandler,
    // Gnosis handlers
    registerSparklendGnosisPoolEventHandlers,
    registerSparklendGnosisPriceCaptureHandler,
    registerSparklendGnosisSnapshotCalculationHandler,
  } = require("@sparklend/handlers");

  // Register Mainnet handlers
  registerSparklendMainnetPoolEventHandlers();
  registerSparklendMainnetSpTokenTransferHandlers();
  registerSparklendMainnetPriceCaptureHandler();
  registerSparklendMainnetSnapshotCalculationHandler();

  // Register Gnosis handlers
  registerSparklendGnosisPoolEventHandlers();
  // registerSparklendGnosisSpTokenTransferHandlers(); // TODO: Add when spToken addresses available
  registerSparklendGnosisPriceCaptureHandler();
  registerSparklendGnosisSnapshotCalculationHandler();

  console.log("✅ Sparklend protocol initialized (Mainnet + Gnosis)");
}
