/**
 * Database Helper Functions
 * 
 * Utilities for creating and ensuring entries exist in reference tables:
 * - Chain, Protocol, Token (common/universal)
 * - AToken, ReserveConfig (Aave V3 architecture)
 */

import type { Context } from "ponder:registry";
import { getTokenMetadata } from "@/utils/token-metadata";
import { Chain, Protocol, Token, User } from "@/schema/common";
import { AToken, ReserveConfig } from "@/schema/aave-v3";

/**
 * Ensure Chain entry exists, create if not
 */
export async function ensureChain(
  context: Context,
  chainId: number,
  chainName: string
): Promise<string> {
  const id = chainName.toLowerCase();
  
  await context.db.insert(Chain)
    .values({
      id,
      chainId,
      name: chainName,
    })
    .onConflictDoNothing();
  
  return id;
}

/**
 * Ensure Protocol entry exists, create if not
 */
export async function ensureProtocol(
  context: Context,
  protocolType: string,
  chainIdentifier: string, // Lowercase chain identifier ("mainnet", "gnosis")
  chainNumericId: number, // Actual chain ID integer (1, 100, etc.)
  chainDisplayName: string, // Display name ("Mainnet", "Gnosis")
  poolAddress: `0x${string}`,
  protocolDisplayName: string
): Promise<string> {
  const normalizedChainId = chainIdentifier.toLowerCase();
  const id = `${protocolType}-${normalizedChainId}`;
  
  // Ensure chain exists first
  await ensureChain(context, chainNumericId, chainDisplayName);
  
  await context.db.insert(Protocol)
    .values({
      id,
      name: protocolDisplayName,
      type: protocolType,
      chainId: normalizedChainId, // References Chain.id (the lowercase string)
      poolAddress,
    })
    .onConflictDoNothing();
  
  return id;
}

/**
 * Ensure Token entry exists, create if not
 * Tokens are chain-scoped (same WETH used by multiple protocols)
 * Fetches metadata from the ERC20 contract on first encounter
 */
export async function ensureToken(
  context: Context,
  chainIdentifier: string, // Lowercase chain identifier ("mainnet", "gnosis")
  address: `0x${string}`,
  blockNumber: bigint,
  timestamp: bigint
): Promise<string> {
  const normalizedChainId = chainIdentifier.toLowerCase();
  const id = `${normalizedChainId}-${address.toLowerCase()}`;
  
  // Fetch metadata from contract
  const metadata = await getTokenMetadata(context, address);
  
  await context.db.insert(Token)
    .values({
      id,
      chainId: normalizedChainId, // References Chain.id (the lowercase string)
      address,
      symbol: metadata.symbol,
      name: metadata.name,
      decimals: metadata.decimals,
      firstSeenBlock: blockNumber,
      firstSeenTimestamp: timestamp,
    })
    .onConflictDoNothing();
  
  return id;
}

/**
 * Ensure AToken entry exists, create if not
 * ATokens are protocol-scoped (spWETH vs aEthWETH)
 * Fetches metadata from the ERC20 contract on first encounter
 */
export async function ensureAToken(
  context: Context,
  protocolId: string,
  tokenId: string,
  address: `0x${string}`,
  blockNumber: bigint,
  timestamp: bigint
): Promise<string> {
  const id = `${protocolId}-${address.toLowerCase()}`;
  
  // Fetch metadata from contract
  const metadata = await getTokenMetadata(context, address);
  
  await context.db.insert(AToken)
    .values({
      id,
      protocolId,
      tokenId,
      address,
      symbol: metadata.symbol,
      name: metadata.name,
      decimals: metadata.decimals,
      firstSeenBlock: blockNumber,
      firstSeenTimestamp: timestamp,
    })
    .onConflictDoNothing();
  
  return id;
}

/**
 * Ensure User entry exists, create if not
 * Users are chain-scoped (not protocol-scoped)
 */
export async function ensureUser(
  context: Context,
  chainIdentifier: string, // Lowercase chain identifier ("mainnet", "gnosis") - references Chain.id
  address: `0x${string}`,
  blockNumber: bigint,
  timestamp: bigint
): Promise<string> {
  const normalizedChainId = chainIdentifier.toLowerCase();
  const id = `${normalizedChainId}-${address.toLowerCase()}`;
  
  await context.db.insert(User)
    .values({
      id,
      chainId: normalizedChainId, // This references Chain.id (which is the lowercase string, not integer!)
      address,
      firstSeenBlock: blockNumber,
      firstSeenTimestamp: timestamp,
      lastActivityBlock: blockNumber,
      lastActivityTimestamp: timestamp,
    })
    .onConflictDoNothing();
  
  return id;
}

/**
 * Create ReserveConfig entry (Aave V3 architecture)
 * These are created for each ReserveDataUpdated event
 */
export async function createReserveConfig(
  context: Context,
  protocolId: string,
  tokenId: string, // Full token ID with chain (e.g., "gnosis-0x...")
  blockNumber: bigint,
  timestamp: bigint,
  transactionHash: string,
  liquidityRate: bigint,
  stableBorrowRate: bigint,
  variableBorrowRate: bigint,
  liquidityIndex: bigint,
  variableBorrowIndex: bigint
): Promise<string> {
  // Extract just the address from tokenId to avoid duplicating chain identifier
  // tokenId format: "chainIdentifier-0xaddress" → extract "0xaddress"
  const tokenAddress = tokenId.includes('-') ? tokenId.split('-').slice(1).join('-') : tokenId;
  const id = `${protocolId}-${tokenAddress}-${blockNumber}`;
  
  // Upsert to handle duplicate events in same block
  await context.db.insert(ReserveConfig)
    .values({
      id,
      protocolId,
      tokenId, // Store the full tokenId for FK reference
      blockNumber,
      timestamp,
      transactionHash,
      liquidityRate,
      stableBorrowRate,
      variableBorrowRate,
      liquidityIndex,
      variableBorrowIndex,
    })
    .onConflictDoNothing();
  
  return id;
}

/**
 * Helper to get token ID from address and chain
 */
export function getTokenId(chainIdentifier: string, address: `0x${string}`): string {
  return `${chainIdentifier.toLowerCase()}-${address.toLowerCase()}`;
}

/**
 * Helper to get protocol ID
 */
export function getProtocolId(protocolType: string, chainIdentifier: string): string {
  return `${protocolType}-${chainIdentifier.toLowerCase()}`;
}

/**
 * Helper to get user ID
 */
export function getUserId(chainIdentifier: string, address: `0x${string}`): string {
  return `${chainIdentifier.toLowerCase()}-${address.toLowerCase()}`;
}

/**
 * Extract address from a full ID that contains chain identifier
 * Format: "chainIdentifier-0xaddress" → "0xaddress"
 */
export function extractAddressFromId(id: string): string {
  return id.includes('-') ? id.split('-').slice(1).join('-') : id;
}

// Legacy aliases for backward compatibility during migration
export const ensureReserve = ensureToken;
export const getReserveId = getTokenId;
export const createReserveConfiguration = createReserveConfig;
