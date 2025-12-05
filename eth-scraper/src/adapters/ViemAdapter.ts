import { createPublicClient, http, PublicClient, Block } from 'viem';
import { mainnet } from 'viem/chains';
import { IChainClient, BlockData } from '../interfaces/IChainClient';

export class ViemAdapter implements IChainClient {
    private client: PublicClient;

    constructor(rpcUrl: string) {
        this.client = createPublicClient({
            chain: mainnet,
            transport: http(rpcUrl),
        });
    }

    async getBlockNumber(): Promise<bigint> {
        return await this.client.getBlockNumber();
    }

    async getBlock(blockNumber: bigint): Promise<BlockData | null> {
        try {
            const block = await this.client.getBlock({
                blockNumber: blockNumber,
            });

            if (!block) return null;

            return {
                number: block.number,
                hash: block.hash,
                timestamp: block.timestamp,
                transactions: block.transactions as string[],
            };
        } catch (error) {
            console.error(`Error fetching block ${blockNumber}:`, error);
            return null;
        }
    }
}
