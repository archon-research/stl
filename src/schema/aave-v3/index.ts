/**
 * Aave V3 Architecture Schema Tables
 * 
 * Shared tables for Aave V3-like protocols (Aave, Sparklend, and forks).
 */

export { AToken, aTokenRelations } from "./atoken";
export { ReserveConfig, reserveConfigRelations } from "./reserve-config";
export { createUserPositionBreakdownTable } from "./user-position-breakdown";
