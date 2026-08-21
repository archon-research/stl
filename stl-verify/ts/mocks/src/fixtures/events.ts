/**
 * Decoded protocol events.
 *
 * Staging's capture was the single `Supply` log of {@link SPARK_TX_HASH}. Two
 * sibling logs from the same transaction are added here, because the tx-detail
 * screen is a list and a one-row list does not show whether the list works. Two
 * further transactions are seeded so the feed's `tx_hash` and `protocol_name`
 * filters have more than one hash to choose between. Every hash is one an
 * activity row carries, with a protocol that agrees.
 */
import { MINUTE_MS, SECOND_MS, isoAgo } from '../clock.ts';
import type { ProtocolEvent } from '../schema.ts';
import { SPARK_TX_HASH } from './allocations.ts';
import { USDS } from './registry.ts';

/** Checksummed: `event_data` carries raw ABI values, not normalised ones. */
const SPARK_PROXY_CHECKSUMMED = '0x1601843c5E9bC251A3272907010AFa41Fa18347E';
const USDS_CHECKSUMMED = '0xdC035D45d973E3EC169d2276DDab16f1e407384F';
const SPARKLEND_POOL = '0xc13e21b648a5ee794902342038ff3adab66be987';

const SUPPLY_TX_AGO = 2 * MINUTE_MS + 47 * SECOND_MS;

/**
 * The two other seeded transactions, so the feed's `tx_hash` and `protocol_name`
 * filters have more than one hash to choose between. They are the hashes the
 * matching activity rows carry, and the protocols there agree with the events
 * decoded here.
 */
const MORPHO_SUPPLY_TX_HASH =
  '0x6ee15ae58c284dd3827ce7924e9e6fede5fc76d756e852441a32e3673f813a95';
const SPARK_WITHDRAW_TX_HASH =
  '0x43019395d99015a53120b8dea9aa964ff4ff6c4ac2437c3ed00a13eae61b227b';

export function seedProtocolEvents(nowMs: number): ProtocolEvent[] {
  const createdAt = isoAgo(nowMs, SUPPLY_TX_AGO);
  const supplyTx = {
    tx_hash: SPARK_TX_HASH,
    chain_id: 1,
    block_number: 25780106,
    block_version: 0,
    protocol_name: 'SparkLend',
    contract_address: SPARKLEND_POOL,
    created_at: createdAt,
  } as const;

  return [
    {
      ...supplyTx,
      log_index: 244,
      event_name: 'Supply',
      event_data: {
        user: SPARK_PROXY_CHECKSUMMED,
        amount: '23582135953817742262321',
        reserve: USDS_CHECKSUMMED,
        eventType: 'Supply',
      },
    },
    {
      ...supplyTx,
      log_index: 243,
      event_name: 'ReserveDataUpdated',
      event_data: {
        reserve: USDS_CHECKSUMMED,
        liquidityRate: '48213500000000000000000000',
        variableBorrowRate: '61904200000000000000000000',
        liquidityIndex: '1041938204718392018492013',
        variableBorrowIndex: '1063920481028301948201983',
        eventType: 'ReserveDataUpdated',
      },
    },
    {
      ...supplyTx,
      log_index: 242,
      event_name: 'Transfer',
      contract_address: USDS,
      event_data: {
        from: SPARK_PROXY_CHECKSUMMED,
        to: '0xC02aB1A5eAA8d1B114EF786D9bde108cD4364359',
        value: '23582135953817742262321',
        eventType: 'Transfer',
      },
    },
    {
      tx_hash: MORPHO_SUPPLY_TX_HASH,
      chain_id: 1,
      block_number: 25779996,
      block_version: 0,
      protocol_name: 'Morpho Blue',
      contract_address: '0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb',
      created_at: isoAgo(nowMs, SUPPLY_TX_AGO + 24 * MINUTE_MS),
      log_index: 118,
      event_name: 'SupplyCollateral',
      event_data: {
        id: '0x3a85e619751152991742810df6ec69ce473daef99e28a64ab2340d7b7ccfee49',
        caller: SPARK_PROXY_CHECKSUMMED,
        onBehalf: SPARK_PROXY_CHECKSUMMED,
        assets: '1500000000000',
        eventType: 'SupplyCollateral',
      },
    },
    {
      tx_hash: SPARK_WITHDRAW_TX_HASH,
      chain_id: 1,
      block_number: 25779886,
      block_version: 0,
      protocol_name: 'SparkLend',
      contract_address: SPARKLEND_POOL,
      created_at: isoAgo(nowMs, SUPPLY_TX_AGO + 48 * MINUTE_MS),
      log_index: 91,
      event_name: 'Withdraw',
      event_data: {
        user: SPARK_PROXY_CHECKSUMMED,
        to: SPARK_PROXY_CHECKSUMMED,
        amount: '750000000000',
        reserve: '0xdAC17F958D2ee523a2206206994597C13D831ec7',
        eventType: 'Withdraw',
      },
    },
  ];
}
