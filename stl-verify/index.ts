import { ethers } from "ethers";
import Database from "better-sqlite3";
import { addressBook, sparklendTokens, tokenByAddress, TokenInfo, sparklendBlocks } from "./addressbook";
import sparkLendPoolAbi from "./abi/sparklendpool.json";
import aaveOracleAbi from "./abi/aaveoracle.json";

// Event types for SparkLend Pool
interface SupplyEvent {
    blockNumber: number;
    transactionHash: string;
    logIndex: number;
    reserve: string;
    user: string;
    onBehalfOf: string;
    amount: string;
    referralCode: number;
}

interface BorrowEvent {
    blockNumber: number;
    transactionHash: string;
    logIndex: number;
    reserve: string;
    user: string;
    onBehalfOf: string;
    amount: string;
    interestRateMode: number;
    borrowRate: string;
    referralCode: number;
}

interface LiquidationCallEvent {
    blockNumber: number;
    transactionHash: string;
    logIndex: number;
    collateralAsset: string;
    debtAsset: string;
    user: string;
    debtToCover: string;
    liquidatedCollateralAmount: string;
    liquidator: string;
    receiveAToken: boolean;
}

interface WithdrawEvent {
    blockNumber: number;
    transactionHash: string;
    logIndex: number;
    reserve: string;
    user: string;
    to: string;
    amount: string;
}

interface RepayEvent {
    blockNumber: number;
    transactionHash: string;
    logIndex: number;
    reserve: string;
    user: string;
    repayer: string;
    amount: string;
    useATokens: boolean;
}

interface SparkLendEvents {
    supply: SupplyEvent[];
    borrow: BorrowEvent[];
    liquidationCall: LiquidationCallEvent[];
    withdraw: WithdrawEvent[];
    repay: RepayEvent[];
}

// Database setup
function initDatabase(dbPath: string = "./sparklend_events.db"): Database.Database {
    const db = new Database(dbPath);

    // Create tables
    db.exec(`
        CREATE TABLE IF NOT EXISTS supply_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number INTEGER NOT NULL,
            transaction_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            reserve TEXT NOT NULL,
            user TEXT NOT NULL,
            on_behalf_of TEXT NOT NULL,
            amount TEXT NOT NULL,
            referral_code INTEGER NOT NULL,
            UNIQUE(transaction_hash, log_index)
        );

        CREATE TABLE IF NOT EXISTS borrow_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number INTEGER NOT NULL,
            transaction_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            reserve TEXT NOT NULL,
            user TEXT NOT NULL,
            on_behalf_of TEXT NOT NULL,
            amount TEXT NOT NULL,
            interest_rate_mode INTEGER NOT NULL,
            borrow_rate TEXT NOT NULL,
            referral_code INTEGER NOT NULL,
            UNIQUE(transaction_hash, log_index)
        );

        CREATE TABLE IF NOT EXISTS liquidation_call_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number INTEGER NOT NULL,
            transaction_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            collateral_asset TEXT NOT NULL,
            debt_asset TEXT NOT NULL,
            user TEXT NOT NULL,
            debt_to_cover TEXT NOT NULL,
            liquidated_collateral_amount TEXT NOT NULL,
            liquidator TEXT NOT NULL,
            receive_atoken INTEGER NOT NULL,
            UNIQUE(transaction_hash, log_index)
        );

        CREATE TABLE IF NOT EXISTS withdraw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number INTEGER NOT NULL,
            transaction_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            reserve TEXT NOT NULL,
            user TEXT NOT NULL,
            to_address TEXT NOT NULL,
            amount TEXT NOT NULL,
            UNIQUE(transaction_hash, log_index)
        );

        CREATE TABLE IF NOT EXISTS repay_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number INTEGER NOT NULL,
            transaction_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            reserve TEXT NOT NULL,
            user TEXT NOT NULL,
            repayer TEXT NOT NULL,
            amount TEXT NOT NULL,
            use_atokens INTEGER NOT NULL,
            UNIQUE(transaction_hash, log_index)
        );

        CREATE TABLE IF NOT EXISTS sync_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_synced_block INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS price_sync_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_synced_block INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS token_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            token_address TEXT NOT NULL,
            token_symbol TEXT NOT NULL,
            price_usd TEXT NOT NULL,
            UNIQUE(block_number, token_address)
        );

        CREATE INDEX IF NOT EXISTS idx_supply_block ON supply_events(block_number);
        CREATE INDEX IF NOT EXISTS idx_supply_user ON supply_events(user);
        CREATE INDEX IF NOT EXISTS idx_borrow_block ON borrow_events(block_number);
        CREATE INDEX IF NOT EXISTS idx_borrow_user ON borrow_events(user);
        CREATE INDEX IF NOT EXISTS idx_liquidation_block ON liquidation_call_events(block_number);
        CREATE INDEX IF NOT EXISTS idx_liquidation_user ON liquidation_call_events(user);
        CREATE INDEX IF NOT EXISTS idx_withdraw_block ON withdraw_events(block_number);
        CREATE INDEX IF NOT EXISTS idx_withdraw_user ON withdraw_events(user);
        CREATE INDEX IF NOT EXISTS idx_repay_block ON repay_events(block_number);
        CREATE INDEX IF NOT EXISTS idx_repay_user ON repay_events(user);
        CREATE INDEX IF NOT EXISTS idx_prices_block ON token_prices(block_number);
        CREATE INDEX IF NOT EXISTS idx_prices_token ON token_prices(token_address);
        CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON token_prices(timestamp);
    `);

    return db;
}

function saveEventsToDatabase(db: Database.Database, events: SparkLendEvents): void {
    const insertSupply = db.prepare(`
        INSERT OR IGNORE INTO supply_events 
        (block_number, transaction_hash, log_index, reserve, user, on_behalf_of, amount, referral_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertBorrow = db.prepare(`
        INSERT OR IGNORE INTO borrow_events 
        (block_number, transaction_hash, log_index, reserve, user, on_behalf_of, amount, interest_rate_mode, borrow_rate, referral_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertLiquidation = db.prepare(`
        INSERT OR IGNORE INTO liquidation_call_events 
        (block_number, transaction_hash, log_index, collateral_asset, debt_asset, user, debt_to_cover, liquidated_collateral_amount, liquidator, receive_atoken)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertWithdraw = db.prepare(`
        INSERT OR IGNORE INTO withdraw_events 
        (block_number, transaction_hash, log_index, reserve, user, to_address, amount)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    `);

    const insertRepay = db.prepare(`
        INSERT OR IGNORE INTO repay_events 
        (block_number, transaction_hash, log_index, reserve, user, repayer, amount, use_atokens)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertMany = db.transaction(() => {
        for (const event of events.supply) {
            insertSupply.run(
                event.blockNumber, event.transactionHash, event.logIndex,
                event.reserve, event.user, event.onBehalfOf, event.amount, event.referralCode
            );
        }
        for (const event of events.borrow) {
            insertBorrow.run(
                event.blockNumber, event.transactionHash, event.logIndex,
                event.reserve, event.user, event.onBehalfOf, event.amount,
                event.interestRateMode, event.borrowRate, event.referralCode
            );
        }
        for (const event of events.liquidationCall) {
            insertLiquidation.run(
                event.blockNumber, event.transactionHash, event.logIndex,
                event.collateralAsset, event.debtAsset, event.user,
                event.debtToCover, event.liquidatedCollateralAmount,
                event.liquidator, event.receiveAToken ? 1 : 0
            );
        }
        for (const event of events.withdraw) {
            insertWithdraw.run(
                event.blockNumber, event.transactionHash, event.logIndex,
                event.reserve, event.user, event.to, event.amount
            );
        }
        for (const event of events.repay) {
            insertRepay.run(
                event.blockNumber, event.transactionHash, event.logIndex,
                event.reserve, event.user, event.repayer, event.amount, event.useATokens ? 1 : 0
            );
        }
    });

    insertMany();
}

function getLastSyncedBlock(db: Database.Database): number | null {
    const row = db.prepare("SELECT last_synced_block FROM sync_state WHERE id = 1").get() as { last_synced_block: number } | undefined;
    return row?.last_synced_block ?? null;
}

function updateLastSyncedBlock(db: Database.Database, blockNumber: number): void {
    db.prepare(`
        INSERT INTO sync_state (id, last_synced_block) VALUES (1, ?)
        ON CONFLICT(id) DO UPDATE SET last_synced_block = excluded.last_synced_block
    `).run(blockNumber);
}

function getLastPriceSyncedBlock(db: Database.Database): number | null {
    const row = db.prepare("SELECT last_synced_block FROM price_sync_state WHERE id = 1").get() as { last_synced_block: number } | undefined;
    return row?.last_synced_block ?? null;
}

function updateLastPriceSyncedBlock(db: Database.Database, blockNumber: number): void {
    db.prepare(`
        INSERT INTO price_sync_state (id, last_synced_block) VALUES (1, ?)
        ON CONFLICT(id) DO UPDATE SET last_synced_block = excluded.last_synced_block
    `).run(blockNumber);
}

// Price data interface
interface TokenPrice {
    blockNumber: number;
    timestamp: number;
    tokenAddress: string;
    tokenSymbol: string;
    priceUsd: string;
}

/**
 * Fetch current prices for all SparkLend tokens from the AaveOracle
 * Prices are returned in USD (base currency unit fetched dynamically from oracle)
 */
async function fetchTokenPrices(
    provider: ethers.JsonRpcProvider,
    blockNumber?: number
): Promise<TokenPrice[]> {
    const oracleAddress = addressBook.sparklend.oracle;
    const oracle = new ethers.Contract(oracleAddress, aaveOracleAbi, provider);

    const tokenAddresses = sparklendTokens.map(t => t.address);
    const blockTag = blockNumber || "latest";

    // Get block info for timestamp
    const block = blockNumber
        ? await provider.getBlock(blockNumber)
        : await provider.getBlock("latest");

    if (!block) {
        throw new Error("Could not fetch block");
    }

    // Fetch BASE_CURRENCY_UNIT to determine decimals dynamically
    const baseCurrencyUnit: bigint = await oracle.BASE_CURRENCY_UNIT({ blockTag });
    const decimals = Math.log10(Number(baseCurrencyUnit));

    // Fetch all prices in a single call
    const prices: bigint[] = await oracle.getAssetsPrices(tokenAddresses, { blockTag });

    const tokenPrices: TokenPrice[] = [];

    for (let i = 0; i < sparklendTokens.length; i++) {
        const token = sparklendTokens[i];
        const priceRaw = prices[i];

        // Convert to USD decimal string using the actual base currency decimals
        const priceUsd = ethers.formatUnits(priceRaw, decimals);

        tokenPrices.push({
            blockNumber: block.number,
            timestamp: block.timestamp,
            tokenAddress: token.address.toLowerCase(),
            tokenSymbol: token.symbol,
            priceUsd
        });
    }

    return tokenPrices;
}

/**
 * Save token prices to database
 */
function savePricesToDatabase(db: Database.Database, prices: TokenPrice[]): void {
    const insertPrice = db.prepare(`
        INSERT OR IGNORE INTO token_prices 
        (block_number, timestamp, token_address, token_symbol, price_usd)
        VALUES (?, ?, ?, ?, ?)
    `);

    const insertMany = db.transaction(() => {
        for (const price of prices) {
            insertPrice.run(
                price.blockNumber,
                price.timestamp,
                price.tokenAddress,
                price.tokenSymbol,
                price.priceUsd
            );
        }
    });

    insertMany();
}

/**
 * Get the latest price for a token from the database
 */
function getLatestPrice(db: Database.Database, tokenAddress: string): TokenPrice | null {
    const row = db.prepare(`
        SELECT block_number, timestamp, token_address, token_symbol, price_usd
        FROM token_prices
        WHERE token_address = ?
        ORDER BY block_number DESC
        LIMIT 1
    `).get(tokenAddress.toLowerCase()) as {
        block_number: number;
        timestamp: number;
        token_address: string;
        token_symbol: string;
        price_usd: string
    } | undefined;

    if (!row) return null;

    return {
        blockNumber: row.block_number,
        timestamp: row.timestamp,
        tokenAddress: row.token_address,
        tokenSymbol: row.token_symbol,
        priceUsd: row.price_usd
    };
}

/**
 * Get price at a specific block (or closest before)
 */
function getPriceAtBlock(db: Database.Database, tokenAddress: string, blockNumber: number): TokenPrice | null {
    const row = db.prepare(`
        SELECT block_number, timestamp, token_address, token_symbol, price_usd
        FROM token_prices
        WHERE token_address = ? AND block_number <= ?
        ORDER BY block_number DESC
        LIMIT 1
    `).get(tokenAddress.toLowerCase(), blockNumber) as {
        block_number: number;
        timestamp: number;
        token_address: string;
        token_symbol: string;
        price_usd: string
    } | undefined;

    if (!row) return null;

    return {
        blockNumber: row.block_number,
        timestamp: row.timestamp,
        tokenAddress: row.token_address,
        tokenSymbol: row.token_symbol,
        priceUsd: row.price_usd
    };
}

/**
 * Sync historical prices from a starting block to current block
 * @param provider - ethers.js provider instance
 * @param db - Database instance
 * @param startBlock - Starting block number
 * @param endBlock - Ending block number
 * @param blockInterval - Interval between price snapshots (default: 100 blocks ≈ 20 minutes)
 */
async function syncHistoricalPrices(
    provider: ethers.JsonRpcProvider,
    db: Database.Database,
    startBlock: number,
    endBlock: number,
    blockInterval: number = 100
): Promise<void> {
    const oracleAddress = addressBook.sparklend.oracle;
    const oracle = new ethers.Contract(oracleAddress, aaveOracleAbi, provider);
    
    // Get BASE_CURRENCY_UNIT once (it doesn't change)
    const baseCurrencyUnit: bigint = await oracle.BASE_CURRENCY_UNIT();
    const decimals = Math.log10(Number(baseCurrencyUnit));
    
    const totalSnapshots = Math.ceil((endBlock - startBlock + 1) / blockInterval);
    
    console.log(`\nSyncing historical prices from block ${startBlock} to ${endBlock}`);
    console.log(`Interval: every ${blockInterval} blocks (~${Math.round(blockInterval * 12 / 60)} minutes)`);
    console.log(`Total snapshots to fetch: ${totalSnapshots}`);
    
    let snapshotCount = 0;
    let blockErrorCount = 0;
    
    for (let blockNumber = startBlock; blockNumber <= endBlock; blockNumber += blockInterval) {
        snapshotCount++;
        
        try {
            // Get block for timestamp
            const block = await provider.getBlock(blockNumber);
            if (!block) {
                console.warn(`Could not fetch block ${blockNumber}, skipping...`);
                blockErrorCount++;
                continue;
            }
            
            const tokenPrices: TokenPrice[] = [];
            
            // Query each token individually to handle tokens that weren't supported yet
            for (const token of sparklendTokens) {
                try {
                    const priceRaw: bigint = await oracle.getAssetPrice(token.address, { blockTag: blockNumber });
                    
                    // Skip tokens with 0 price (not yet supported at this block)
                    if (priceRaw === 0n) continue;
                    
                    const priceUsd = ethers.formatUnits(priceRaw, decimals);
                    
                    tokenPrices.push({
                        blockNumber: block.number,
                        timestamp: block.timestamp,
                        tokenAddress: token.address.toLowerCase(),
                        tokenSymbol: token.symbol,
                        priceUsd
                    });
                } catch {
                    // Token not supported in oracle at this block, skip silently
                }
            }
            
            // Save to database if we got any prices
            if (tokenPrices.length > 0) {
                savePricesToDatabase(db, tokenPrices);
            }
            
            // Update sync state
            updateLastPriceSyncedBlock(db, blockNumber);
            
            // Progress logging every 100 snapshots
            if (snapshotCount % 100 === 0 || blockNumber + blockInterval > endBlock) {
                const progress = ((blockNumber - startBlock) / (endBlock - startBlock) * 100).toFixed(1);
                console.log(`[${progress}%] Block ${blockNumber} - ${tokenPrices.length} prices saved (${snapshotCount}/${totalSnapshots} snapshots)`);
            }
            
        } catch (error) {
            blockErrorCount++;
            if (blockErrorCount <= 10) {
                console.error(`Error at block ${blockNumber}:`, error);
            } else if (blockErrorCount === 11) {
                console.error("Too many block errors, suppressing further error messages...");
            }
        }
    }
    
    console.log(`\nHistorical price sync complete!`);
    console.log(`Snapshots: ${snapshotCount}, Errors: ${blockErrorCount}`);
}

/**
 * Sync historical SparkLend events from a starting block to current block
 * @param provider - ethers.js provider instance
 * @param db - Database instance
 * @param sparklendCreationBlock - Block where SparkLend was deployed
 * @param currentBlock - Current block number
 * @param chunkSize - Number of blocks to process per chunk (default: 10000)
 */
async function syncHistoricalEvents(
    provider: ethers.JsonRpcProvider,
    db: Database.Database,
    sparklendCreationBlock: number,
    currentBlock: number,
    chunkSize: number = 10000
): Promise<void> {
    // Check if we have a previous sync state
    const lastSyncedBlock = getLastSyncedBlock(db);
    const startBlock = lastSyncedBlock ? lastSyncedBlock + 1 : sparklendCreationBlock;

    if (startBlock > currentBlock) {
        console.log("Events already synced to current block. Nothing to do.");
        return;
    }

    const totalChunks = Math.ceil((currentBlock - startBlock + 1) / chunkSize);

    console.log(`\n=== Syncing Historical SparkLend Events ===`);
    console.log(`Current block: ${currentBlock}`);
    console.log(`Starting from block: ${startBlock}`);
    console.log(`Total chunks to process: ${totalChunks}`);

    let totalSupply = 0, totalBorrow = 0, totalLiquidation = 0, totalWithdraw = 0, totalRepay = 0;

    for (let chunkStart = startBlock; chunkStart <= currentBlock; chunkStart += chunkSize) {
        const chunkEnd = Math.min(chunkStart + chunkSize - 1, currentBlock);
        const chunkNumber = Math.floor((chunkStart - startBlock) / chunkSize) + 1;

        console.log(`\n[Chunk ${chunkNumber}/${totalChunks}] Processing blocks ${chunkStart} to ${chunkEnd}...`);

        try {
            const events = await querySparkLendEvents(provider, chunkStart, chunkEnd);

            // Save events to database
            saveEventsToDatabase(db, events);

            // Update sync state
            updateLastSyncedBlock(db, chunkEnd);

            // Track totals
            totalSupply += events.supply.length;
            totalBorrow += events.borrow.length;
            totalLiquidation += events.liquidationCall.length;
            totalWithdraw += events.withdraw.length;
            totalRepay += events.repay.length;

            console.log(`Saved to database. Running totals - Supply: ${totalSupply}, Borrow: ${totalBorrow}, Liquidation: ${totalLiquidation}, Withdraw: ${totalWithdraw}, Repay: ${totalRepay}`);
        } catch (error) {
            console.error(`Error processing chunk ${chunkNumber}:`, error);
            // Continue with next chunk rather than failing completely
        }
    }

    console.log("\n=== SparkLend Events Summary ===");
    console.log(`Supply events: ${totalSupply}`);
    console.log(`Borrow events: ${totalBorrow}`);
    console.log(`LiquidationCall events: ${totalLiquidation}`);
    console.log(`Withdraw events: ${totalWithdraw}`);
    console.log(`Repay events: ${totalRepay}`);
    console.log(`\nEvents saved to sparklend_events.db`);
}

async function testRpcConnection(rpcUrl: string) {
    console.log(`Testing connection to ${rpcUrl}...`);
    try {
        const provider = new ethers.JsonRpcProvider(rpcUrl);
        const network = await provider.getNetwork();
        const blockNumber = await provider.getBlockNumber();
        console.log(`Successfully connected to ${network.name} (Chain ID: ${network.chainId})`);
        console.log(`Current Block Number: ${blockNumber}`);
        return true;
    } catch (error) {
        console.error(`Failed to connect to ${rpcUrl}:`, error);
        return false;
    }
}

/**
 * Query SparkLend Pool events from the Ethereum chain
 * @param provider - ethers.js provider instance
 * @param fromBlock - Starting block number
 * @param toBlock - Ending block number (defaults to 'latest')
 * @returns SparkLendEvents object containing all queried events
 */
async function querySparkLendEvents(
    provider: ethers.JsonRpcProvider,
    fromBlock: number,
    toBlock: number | string = "latest"
): Promise<SparkLendEvents> {
    const poolAddress = addressBook.sparklend.pool;
    const poolContract = new ethers.Contract(poolAddress, sparkLendPoolAbi, provider);

    console.log(`Querying SparkLend events from block ${fromBlock} to ${toBlock}...`);
    console.log(`Pool address: ${poolAddress}`);

    const events: SparkLendEvents = {
        supply: [],
        borrow: [],
        liquidationCall: [],
        withdraw: [],
        repay: []
    };

    try {
        // Query Supply events
        const supplyFilter = poolContract.filters.Supply();
        const supplyLogs = await poolContract.queryFilter(supplyFilter, fromBlock, toBlock);
        events.supply = supplyLogs.map((log) => {
            const parsed = poolContract.interface.parseLog({ topics: log.topics as string[], data: log.data });
            return {
                blockNumber: log.blockNumber,
                transactionHash: log.transactionHash,
                logIndex: log.index,
                reserve: parsed?.args.reserve,
                user: parsed?.args.user,
                onBehalfOf: parsed?.args.onBehalfOf,
                amount: parsed?.args.amount.toString(),
                referralCode: Number(parsed?.args.referralCode)
            } as SupplyEvent;
        });
        console.log(`Found ${events.supply.length} Supply events`);

        // Query Borrow events
        const borrowFilter = poolContract.filters.Borrow();
        const borrowLogs = await poolContract.queryFilter(borrowFilter, fromBlock, toBlock);
        events.borrow = borrowLogs.map((log) => {
            const parsed = poolContract.interface.parseLog({ topics: log.topics as string[], data: log.data });
            return {
                blockNumber: log.blockNumber,
                transactionHash: log.transactionHash,
                logIndex: log.index,
                reserve: parsed?.args.reserve,
                user: parsed?.args.user,
                onBehalfOf: parsed?.args.onBehalfOf,
                amount: parsed?.args.amount.toString(),
                interestRateMode: Number(parsed?.args.interestRateMode),
                borrowRate: parsed?.args.borrowRate.toString(),
                referralCode: Number(parsed?.args.referralCode)
            } as BorrowEvent;
        });
        console.log(`Found ${events.borrow.length} Borrow events`);

        // Query LiquidationCall events
        const liquidationFilter = poolContract.filters.LiquidationCall();
        const liquidationLogs = await poolContract.queryFilter(liquidationFilter, fromBlock, toBlock);
        events.liquidationCall = liquidationLogs.map((log) => {
            const parsed = poolContract.interface.parseLog({ topics: log.topics as string[], data: log.data });
            return {
                blockNumber: log.blockNumber,
                transactionHash: log.transactionHash,
                logIndex: log.index,
                collateralAsset: parsed?.args.collateralAsset,
                debtAsset: parsed?.args.debtAsset,
                user: parsed?.args.user,
                debtToCover: parsed?.args.debtToCover.toString(),
                liquidatedCollateralAmount: parsed?.args.liquidatedCollateralAmount.toString(),
                liquidator: parsed?.args.liquidator,
                receiveAToken: parsed?.args.receiveAToken
            } as LiquidationCallEvent;
        });
        console.log(`Found ${events.liquidationCall.length} LiquidationCall events`);

        // Query Withdraw events
        const withdrawFilter = poolContract.filters.Withdraw();
        const withdrawLogs = await poolContract.queryFilter(withdrawFilter, fromBlock, toBlock);
        events.withdraw = withdrawLogs.map((log) => {
            const parsed = poolContract.interface.parseLog({ topics: log.topics as string[], data: log.data });
            return {
                blockNumber: log.blockNumber,
                transactionHash: log.transactionHash,
                logIndex: log.index,
                reserve: parsed?.args.reserve,
                user: parsed?.args.user,
                to: parsed?.args.to,
                amount: parsed?.args.amount.toString()
            } as WithdrawEvent;
        });
        console.log(`Found ${events.withdraw.length} Withdraw events`);

        // Query Repay events
        const repayFilter = poolContract.filters.Repay();
        const repayLogs = await poolContract.queryFilter(repayFilter, fromBlock, toBlock);
        events.repay = repayLogs.map((log) => {
            const parsed = poolContract.interface.parseLog({ topics: log.topics as string[], data: log.data });
            return {
                blockNumber: log.blockNumber,
                transactionHash: log.transactionHash,
                logIndex: log.index,
                reserve: parsed?.args.reserve,
                user: parsed?.args.user,
                repayer: parsed?.args.repayer,
                amount: parsed?.args.amount.toString(),
                useATokens: parsed?.args.useATokens
            } as RepayEvent;
        });
        console.log(`Found ${events.repay.length} Repay events`);

    } catch (error) {
        console.error("Error querying SparkLend events:", error);
        throw error;
    }

    return events;
}

async function main() {
    const rpcUrl = "http://100.87.11.17:8545";
    await testRpcConnection(rpcUrl);

    // Initialize database
    const db = initDatabase();
    console.log("Database initialized");

    // Create provider and query events
    const provider = new ethers.JsonRpcProvider(rpcUrl);

    // Get current block
    const currentBlock = await provider.getBlockNumber();
    const sparklendCreationBlock = sparklendBlocks.poolCreation;
    const oracleStartBlock = sparklendBlocks.oracleOperational;
    
    // Sync historical events
    await syncHistoricalEvents(provider, db, sparklendCreationBlock, currentBlock);
    
    // Sync historical prices (oracle wasn't operational until block 16776437)
    console.log("\n=== Syncing Historical Token Prices ===");
    const lastPriceSyncedBlock = getLastPriceSyncedBlock(db);
    const priceStartBlock = lastPriceSyncedBlock ? lastPriceSyncedBlock + 1 : oracleStartBlock;
    if (priceStartBlock <= currentBlock) {
        // Sync prices every 100 blocks (~20 minutes on Ethereum)
        await syncHistoricalPrices(provider, db, priceStartBlock, currentBlock, 100);
    } else {
        console.log("Prices already synced to current block.");
    }

    // Display current token prices from database
    console.log("\n=== Current Token Prices ===");
    console.log("-".repeat(50));
    for (const token of sparklendTokens) {
        const price = getLatestPrice(db, token.address);
        if (price) {
            const formattedPrice = parseFloat(price.priceUsd).toLocaleString('en-US', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 6
            });
            console.log(`${price.tokenSymbol.padEnd(8)} $${formattedPrice}`);
        }
    }
    console.log("-".repeat(50));

    // Close database
    db.close();
}

main().catch((error) => {
    console.error(error);
});

export {
    querySparkLendEvents,
    initDatabase,
    saveEventsToDatabase,
    getLastSyncedBlock,
    updateLastSyncedBlock,
    fetchTokenPrices,
    savePricesToDatabase,
    getLatestPrice,
    getPriceAtBlock,
    syncHistoricalPrices,
    syncHistoricalEvents,
    getLastPriceSyncedBlock,
    updateLastPriceSyncedBlock,
    SparkLendEvents,
    SupplyEvent,
    BorrowEvent,
    LiquidationCallEvent,
    WithdrawEvent,
    RepayEvent,
    TokenPrice
};
