import { ViemAdapter } from './src/adapters/ViemAdapter';
import { Scraper } from './src/services/Scraper';

async function main() {
    // Your archive node URL
    const ARCHIVE_NODE_URL = 'http://100.87.11.17:8545';

    // Initialize the adapter (Dependency Injection)
    // We can easily swap this for AlchemyAdapter or InfuraAdapter later
    const chainClient = new ViemAdapter(ARCHIVE_NODE_URL);

    // Initialize the domain service
    const scraper = new Scraper(chainClient);

    try {
        console.log('Starting scraper...');
        await scraper.getLatestBlockInfo();

        // Example: Scrape a small range
        // const currentBlock = await chainClient.getBlockNumber();
        // await scraper.scrapeRange(currentBlock - 5n, currentBlock);
        
    } catch (error) {
        console.error('Application error:', error);
    }
}

main();
