/**
 * Ponder Schema
 *
 * Imports and re-exports all schemas:
 * - Common tables (Chain, Protocol, Token) - universal across all protocols
 * - Aave V3 tables (YieldToken, ReserveConfig) - shared by Aave V3-like protocols
 * - Protocol-specific event/state tables
 * 
 * Protocols:
 * - Sparklend: Spark Protocol lending markets (Mainnet, Gnosis)
 * - Aave: Aave V3 Core and Horizon markets (Mainnet)
 */

// Common Reference Tables (universal)
export * from "@/schema/common";

// Aave V3 Architecture Tables (shared by Aave, Sparklend, etc.)
export * from "@/schema/aave-v3";

// Sparklend Protocol Schemas
export * from "@sparklend/schema";

// Aave Protocol Schemas
export * from "@aave/schema";
