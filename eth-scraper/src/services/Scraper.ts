import { IChainClient } from '../interfaces/IChainClient';

export class Scraper {
    private client: IChainClient;

    constructor(client: IChainClient) {
        this.client = client;
    }

    async getLatestBlockInfo(): Promise<void> {
        const blockNumber = await this.client.getBlockNumber();
        console.log(`Latest block number: ${blockNumber}`);
        
        const block = await this.client.getBlock(blockNumber);
        if (block) {
            console.log(`Block Hash: ${block.hash}`);
            console.log(`Timestamp: ${block.timestamp}`);
            console.log(`Transaction count: ${block.transactions.length}`);
        }
    }

    async scrapeRange(startBlock: bigint, endBlock: bigint): Promise<void> {
        console.log(`Scraping from ${startBlock} to ${endBlock}...`);
        for (let i = startBlock; i <= endBlock; i++) {
            const block = await this.client.getBlock(i);
            if (block) {
                // Here you would typically save to a DB
                console.log(`Processed block ${i} with ${block.transactions.length} txs`);
            }
        }
    }
}
