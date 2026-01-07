/**
 * Aave Protocol Schema
 * 
 * Main entry point for all Aave protocol database schemas.
 * Re-exports chain-specific and factory schemas.
 */

// Chain-specific schemas
export * from "./chains";

// Schema factories (for creating new chain schemas)
export * from "./factory";


