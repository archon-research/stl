export interface BlockData {
    number: bigint;
    hash: string;
    timestamp: bigint;
    transactions: string[]; // Or full transaction objects if needed
}

export interface IChainClient {
    getBlockNumber(): Promise<bigint>;
    getBlock(blockNumber: bigint): Promise<BlockData | null>;
    // Add more methods as needed, e.g., getLogs, getTransaction, etc.
}
