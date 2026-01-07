/**
 * Protocol Indexer Entry Point
 * 
 * Imports all protocol handlers to register them with Ponder.
 * Add new protocols here as they are implemented.
 */

// Import protocol handlers
import "./protocols/sparklend/handlers";
import "./protocols/aave/handlers";

console.log("✅ All protocol handlers registered");
