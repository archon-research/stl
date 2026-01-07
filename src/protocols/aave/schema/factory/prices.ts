import { onchainTable } from "ponder";

/**
 * Aave Price Schema Factory
 *
 * Generates chain-specific price snapshot tables for both Core and Horizon markets.
 * All price tables reference normalized Protocol and Token tables.
 * 
 * TODO: These tables will be populated when block-based price capture is implemented.
 */

export function createAavePriceTables(chainName: string, market: "Core" | "Horizon") {
  const prefix = `Aave${chainName}${market}`;

  return {
    // Asset price snapshot - captured daily via blocks handler
    AssetPriceSnapshot: onchainTable(`${prefix}AssetPriceSnapshot`, (t) => ({
      id: t.text().primaryKey(), // `aave-${market}-${chain}-${asset}-${blockNumber}`
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      priceUSD: t.bigint().notNull(), // Price in USD with 8 decimals (Aave oracle format)
    })),
  };
}

