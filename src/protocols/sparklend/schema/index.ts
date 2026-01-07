/**
 * Sparklend Protocol Schema
 *
 * Exports all table definitions for Sparklend protocol across all chains.
 * Each chain has its own isolated set of tables.
 */

// Export all chain-specific schemas
export * from "./chains";

// Export factory functions for potential future use
export * from "./factory";
