/**
 * Configuration Utilities
 * 
 * Helper functions for accessing runtime configuration.
 */

/**
 * Get database configuration based on environment variables
 */
export const getDatabaseConfig = () => {
  if (process.env.DATABASE_URL) {
    return {
      kind: "postgres" as const,
      connectionString: process.env.DATABASE_URL,
    };
  }
  return {
    kind: "pglite" as const,
  };
};

/**
 * Get RPC URL for a specific chain
 */
export const getRpcUrl = (chainId: number): string | undefined => {
  const rpcKey = `PONDER_RPC_URL_${chainId}`;
  return process.env[rpcKey];
};


