/**
 * Position identity, ported from `python/app/domain/position_identity.py`.
 *
 * Both provenance-carrying endpoints publish `position_keys` so a client can
 * attach a risk figure to an allocation row without restating these rules; the
 * mocks have to publish them too, or offline mode shows every requirement as
 * unavailable. When the Python file changes, this one is wrong until it is
 * changed too.
 */

/** A 0x-prefixed 20-byte address. A Uniswap V4 pool id is 66 chars and is not one. */
const ADDRESS_LENGTH = 42;

const CUSTODY_PROTOCOL = 'anchorage';

export type PositionFacts = {
  chain_id: number | null | undefined;
  network?: string | null;
  position_address: string | null | undefined;
  receipt_token_id: number | null | undefined;
  protocol_name: string | null | undefined;
  symbol: string;
};

export function positionKeys(facts: PositionFacts): string[] {
  if ((facts.protocol_name ?? '').toLowerCase() === CUSTODY_PROTOCOL) {
    return [`custody:${CUSTODY_PROTOCOL}`];
  }

  const candidates: string[] = [];
  if (facts.receipt_token_id != null) {
    candidates.push(`token:${facts.receipt_token_id}`);
  }

  const chain = chainKey(facts);
  const address = (facts.position_address ?? '').toLowerCase();
  if (chain !== null && address.length === ADDRESS_LENGTH) {
    candidates.push(`position:${chain}:${address}`);
  }

  if (candidates.length > 0) return candidates;
  if (chain === null) return [];

  return [
    `symbol:${chain}:${facts.protocol_name ?? ''}:${facts.symbol.toLowerCase()}`,
  ];
}

function chainKey(facts: PositionFacts): string | null {
  if (facts.chain_id != null) return String(facts.chain_id);
  return facts.network ? `net:${facts.network}` : null;
}
