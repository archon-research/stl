import { createConfig } from "ponder";
import { http } from "viem";
import { sparklendContracts, sparklendBlocks } from "@sparklend/config";
import { aaveContracts, aaveBlocks } from "@aave/config";

/**
 * Ponder Configuration
 *
 * Imports protocol-specific configurations from modular protocol folders.
 * Each protocol is self-contained with its own ABIs, schemas, handlers, and utilities.
 * 
 * Protocols:
 * - Sparklend: Spark Protocol lending markets (Mainnet, Gnosis)
 * - Aave: Aave V3 Core and Horizon markets (Mainnet)
 */
export default createConfig({
  database: process.env.DATABASE_URL
    ? {
        kind: "postgres",
        connectionString: process.env.DATABASE_URL,
      }
    : {
        kind: "pglite",
      },
  chains: {
    mainnet: {
      id: 1,
      rpc: http(process.env.PONDER_RPC_URL_1),
    },
    gnosis: {
      id: 100,
      rpc: http(process.env.PONDER_RPC_URL_100),
    },
  },
  contracts: {
    // Sparklend Protocol Contracts
    ...sparklendContracts,
    // Aave Protocol Contracts
    ...aaveContracts,
  },
  blocks: {
    // Sparklend Protocol Block Triggers
    ...sparklendBlocks,
    // Aave Protocol Block Triggers
    ...aaveBlocks,
  },
});