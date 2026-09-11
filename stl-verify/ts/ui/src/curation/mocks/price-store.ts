/**
 * Price observations, in their own module.
 *
 * Separate from the append stores on purpose, and not only for line count:
 * prices are the fast-moving side ADR-0007 keeps out of the graph entirely, and
 * they carry no valid-time window, no processing_version and no content hash.
 * Modelling them with the node/edge machinery would imply a bitemporality they
 * do not have.
 *
 * Still append-only in the sense that matters — a second observation for the
 * same instant is a second row, not an overwrite — because that is what makes a
 * restatement visible rather than silent.
 */
import type { PriceRow } from '../lib/contract.ts';

let priceRows: PriceRow[] = [];
let priceSeq = 0;

export function resetPrices(): void {
  priceRows = [];
  priceSeq = 0;
}

export type PriceQuery = {
  chainId?: number;
  tokenAddress?: string;
  limit?: number;
};

export function listPrices(query: PriceQuery): PriceRow[] {
  let rows = [...priceRows];

  if (query.chainId !== undefined) {
    rows = rows.filter((r) => r.chain_id === query.chainId);
  }

  if (query.tokenAddress !== undefined) {
    const needle = query.tokenAddress.toLowerCase();
    rows = rows.filter((r) => r.token_address.toLowerCase() === needle);
  }

  return rows
    .sort((a, b) => b.observed_at.localeCompare(a.observed_at))
    .slice(0, query.limit ?? 200);
}

export type PriceBatchResult = {
  accepted: number;
  rejected: { line: number; message: string }[];
  firstRecordId: number | null;
};

function str(value: unknown, fallback: string): string {
  return typeof value === 'string' ? value : fallback;
}

/**
 * Accepts a batch of price rows, judging each one on its own.
 *
 * A rejected row does not stop the batch: the client already validated every row
 * against the schema and let the curator exclude the ones it could not fix, so
 * anything failing here is a rule only the server holds. Failing the whole batch
 * for one of them would discard work the curator has already reviewed.
 */
export function appendPrices(
  rows: readonly Record<string, unknown>[],
): PriceBatchResult {
  const rejected: { line: number; message: string }[] = [];
  let accepted = 0;
  let firstRecordId: number | null = null;

  for (const [index, row] of rows.entries()) {
    const chainValue = row['chain_id'];
    const chain =
      typeof chainValue === 'number' ? chainValue : Number(chainValue);
    const address = str(row['token_address'], '').toLowerCase();
    const observedAt = str(row['observed_at'], '');
    const price = str(row['price_usd'], '');
    const source = str(row['source_name'], '');

    if (
      !Number.isFinite(chain) ||
      address === '' ||
      observedAt === '' ||
      price === ''
    ) {
      rejected.push({
        line: index + 1,
        message: 'missing chain, address, timestamp or price',
      });
      continue;
    }

    // The one rule the client cannot know: whether this instant already carries
    // an observation from the same source. Two prices for one instant make the
    // series unanswerable, so the duplicate is reported rather than stored.
    const clash = priceRows.some(
      (r) =>
        r.chain_id === chain &&
        r.token_address === address &&
        r.observed_at === observedAt &&
        r.source_name === source,
    );

    if (clash) {
      rejected.push({
        line: index + 1,
        message: `already an observation for ${observedAt} from this source`,
      });
      continue;
    }

    priceSeq += 1;
    firstRecordId ??= priceSeq;
    priceRows.push({
      chain_id: chain,
      token_address: address,
      observed_at: observedAt,
      price_usd: price,
      source_name: source,
      record_id: priceSeq,
      ingested_at: new Date().toISOString(),
      actor: 'curator:local',
      change_reason_code: str(row['change_reason_code'], 'CURATED_SOURCE'),
    });
    accepted += 1;
  }

  return { accepted, rejected, firstRecordId };
}
