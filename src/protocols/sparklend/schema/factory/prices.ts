import { onchainTable } from "ponder";

/**
 * Sparklend Price Schema Factory
 *
 * Generates chain-specific price snapshot tables.
 * All price tables reference normalized Protocol and Token tables.
 */

export function createSparklendPriceTables(chainName: string) {
  const prefix = `Sparklend${chainName}`;

  return {
    // Asset price snapshot - captured daily via blocks handler
    AssetPriceSnapshot: onchainTable(`${prefix}AssetPriceSnapshot`, (t) => ({
      id: t.text().primaryKey(), // `sparklend-${chain}-${asset}-${blockNumber}`
      protocolId: t.text().notNull(),
      reserveId: t.text().notNull(),
      blockNumber: t.bigint().notNull(),
      timestamp: t.bigint().notNull(),
      priceUSD: t.bigint().notNull(), // Price in USD with 8 decimals (Aave oracle format)
    })),
  };
}
