/**
 * RPC Provider factory for multi-chain support
 */

import { ethers } from "ethers";
import { type ChainId, getChainConfig } from "../config/chains";

// Default public RPC endpoints (fallbacks)
const defaultRpcUrls: Record<ChainId, string> = {
    ethereum: "https://eth.llamarpc.com",
    gnosis: "https://rpc.gnosischain.com"
};

/**
 * Get RPC URL for a chain from environment or use default
 */
export function getRpcUrl(chainId: ChainId): string {
    const config = getChainConfig(chainId);
    const envUrl = Bun.env[config.rpcEnvVar];
    return envUrl || defaultRpcUrls[chainId];
}

/**
 * Create an ethers.js provider for a chain
 */
export function createProvider(chainId: ChainId): ethers.JsonRpcProvider {
    const rpcUrl = getRpcUrl(chainId);
    return new ethers.JsonRpcProvider(rpcUrl);
}

/**
 * Test connection to an RPC endpoint
 */
export async function testRpcConnection(chainId: ChainId): Promise<boolean> {
    const rpcUrl = getRpcUrl(chainId);
    const config = getChainConfig(chainId);

    console.log(`Testing connection to ${config.name} (${rpcUrl})...`);

    try {
        const provider = new ethers.JsonRpcProvider(rpcUrl);
        const network = await provider.getNetwork();
        const blockNumber = await provider.getBlockNumber();

        console.log(`  Connected to ${network.name} (Chain ID: ${network.chainId})`);
        console.log(`  Current Block: ${blockNumber}`);

        // Verify chain ID matches expected
        if (Number(network.chainId) !== config.chainId) {
            console.warn(`Warning: Expected chain ID ${config.chainId}, got ${network.chainId}`);
        }

        return true;
    } catch (error) {
        console.error(`Failed to connect to ${config.name}:`, error);
        return false;
    }
}

/**
 * Multicall3 contract address (same on all major chains)
 */
export const MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11";

export const MULTICALL3_ABI = [
    "function aggregate3(tuple(address target, bool allowFailure, bytes callData)[] calls) view returns (tuple(bool success, bytes returnData)[])"
];

/**
 * Create a Multicall3 contract instance
 */
export function createMulticall(provider: ethers.JsonRpcProvider): ethers.Contract {
    return new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, provider);
}
