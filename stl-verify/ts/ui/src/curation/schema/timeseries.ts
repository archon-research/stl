import * as z from 'zod';

import { chainId, evmAddress, exactDecimal, ui } from './primitives.ts';
import { checkProvenance, provenanceInput } from './provenance.ts';

/**
 * A dated price observation, as a write schema.
 *
 * **Provisional.** There is no time-series write contract anywhere yet, so this
 * is derived from the shapes the *read* API already publishes rather than
 * invented: `TokenPriceResponse.price_usd` is documented as "Decimal serialized
 * as a JSON string to preserve precision", `TimeSeriesWindow` timestamps are
 * UTC ISO date-times, and a price carries `source_name` / `source_id`. When the
 * Time-Series Data API PRD's §5.3/§5.4 row shape lands, this file is what
 * changes — nothing downstream of it should need to.
 *
 * Addressing is the API's own natural key, `(chain_id, token_address)`, which is
 * also the repo convention for the `token` registry. TS-4's preferred
 * `instrument_key` addressing is blocked on SECstore metadata, so it is not
 * offered here; when it arrives it becomes an alternative discriminator on this
 * schema rather than a second one.
 */
export const pricePoint = z
  .object({
    chain_id: ui(chainId, {
      label: 'Chain',
      help: 'EVM chain id; part of the token’s natural key',
      group: 'Series',
      order: 1,
    }),
    token_address: ui(evmAddress, {
      label: 'Token address',
      help: 'The contract address, lower-cased on the way in',
      group: 'Series',
      order: 2,
    }),
    observed_at: ui(z.iso.datetime({ offset: true }), {
      label: 'Observed at',
      help: 'UTC instant the quote is for, ISO-8601. An offset is accepted and normalised',
      group: 'Observation',
      order: 1,
    }),
    price_usd: ui(exactDecimal, {
      widget: 'decimal',
      label: 'Price (USD)',
      help: 'Exact decimal as a string — a float round-trip loses precision the store keeps',
      group: 'Observation',
      order: 2,
    }),
    source_name: ui(z.string().trim().min(2).max(64), {
      label: 'Source',
      help: 'Machine name of the feed, e.g. coingecko',
      group: 'Observation',
      order: 3,
    }),
    ...provenanceInput.shape,
  })
  .check((ctx) => {
    checkProvenance(ctx.value, ctx);
  });

export type PricePoint = z.infer<typeof pricePoint>;

/**
 * The columns a file is expected to carry, in the order a template would list
 * them.
 *
 * Derived from the schema rather than written out, so a field added above shows
 * up in the template and in the column check without a second edit.
 */
export const PRICE_POINT_COLUMNS = Object.keys(pricePoint.def.shape);
