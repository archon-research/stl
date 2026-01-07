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
  chainName: string,
  poolAddress: `0x${string}`,
  protocolDisplayName: string
): Promise<string> {
  const chainId = chainName.toLowerCase();
  const id = `${protocolType}-${chainId}`;
  
  // Get numeric chain ID
  const numericChainId = chainName.toLowerCase() === "mainnet" ? 1 : 100;
  
  // Ensure chain exists first
  await ensureChain(context, numericChainId, chainName);
  
  await context.db.insert(Protocol)
    .values({
      id,
      name: protocolDisplayName,
      type: protocolType,
      chainId,
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
  chainName: string,
  address: `0x${string}`,
  blockNumber: bigint,
  timestamp: bigint
): Promise<string> {
  const chainId = chainName.toLowerCase();
  const id = `${chainId}-${address.toLowerCase()}`;
  
  // Fetch metadata from contract
  const metadata = await getTokenMetadata(context, address);
  
  await context.db.insert(Token)
    .values({
      id,
      chainId,
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
  chainName: string,
  address: `0x${string}`,
  blockNumber: bigint,
  timestamp: bigint
): Promise<string> {
  const chainId = chainName.toLowerCase();
  const id = `${chainId}-${address.toLowerCase()}`;
  
  await context.db.insert(User)
    .values({
      id,
      chainId,
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
  tokenId: string,
  blockNumber: bigint,
  timestamp: bigint,
  transactionHash: string,
  liquidityRate: bigint,
  stableBorrowRate: bigint,
  variableBorrowRate: bigint,
  liquidityIndex: bigint,
  variableBorrowIndex: bigint
): Promise<string> {
  const id = `${protocolId}-${tokenId}-${blockNumber}`;
  
  // Upsert to handle duplicate events in same block
  await context.db.insert(ReserveConfig)
    .values({
      id,
      protocolId,
      tokenId,
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
export function getTokenId(chainName: string, address: `0x${string}`): string {
  return `${chainName.toLowerCase()}-${address.toLowerCase()}`;
}

/**
 * Helper to get protocol ID
 */
export function getProtocolId(protocolType: string, chainName: string): string {
  return `${protocolType}-${chainName.toLowerCase()}`;
}

/**
 * Helper to get user ID
 */
export function getUserId(chainName: string, address: `0x${string}`): string {
  return `${chainName.toLowerCase()}-${address.toLowerCase()}`;
}

// Legacy aliases for backward compatibility during migration
export const ensureReserve = ensureToken;
export const getReserveId = getTokenId;
export const createReserveConfiguration = createReserveConfig;
