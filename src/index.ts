/**
 * Protocol Indexer Entry Point
 * 
 * Imports all protocol handlers to register them with Ponder.
 * Handlers are automatically registered on import.
 * Add new protocols here as they are implemented.
 */

// Import protocol handlers (auto-registers)
import "./protocols/sparklend/handlers";
import "./protocols/aave/handlers";

console.log("✅ All protocol handlers registered");
