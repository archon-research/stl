/**
 * Risk capital, the collateral breakdown, and RRC.
 */
import { mockDelay } from '@archon-research/http-client-msw';
import type { MockHandler } from '@archon-research/http-client-msw';

import {
  RISK_CAPITAL_BY_PROXY,
  breakdownFor,
  poolShareFor,
  rrcEnvelope,
  scaleBreakdown,
  toCompositeRiskCapital,
  toReferenceRiskCapital,
} from '../fixtures/risk.ts';
import { ownEntry } from '../lookup.ts';
import { LIST_DELAY_MS, mock } from '../mock-api.ts';
import {
  invalidQueryParam,
  notFound,
  problemResponse,
  unavailable,
} from '../problem.ts';
import { readChainId, readProvenance } from '../query.ts';

export function riskHandlers(): MockHandler[] {
  return [
    mock.get(
      '/v1/primes/{prime_id}/risk-capital',
      async ({ params, query, response }) => {
        await mockDelay(LIST_DELAY_MS);
        const source = readProvenance(
          query.get('source'),
          query.get('reference'),
        );
        if (!source.ok) {
          return response.untyped(problemResponse(source.problem));
        }
        const selfScoped = ownEntry(
          RISK_CAPITAL_BY_PROXY,
          params.prime_id.toLowerCase(),
        );

        if (selfScoped === undefined) {
          return response.untyped(
            problemResponse(notFound(`Prime not found: ${params.prime_id}`)),
          );
        }

        if (source.value === 'reference') {
          return response(200).json(toReferenceRiskCapital(selfScoped));
        }
        return response(200).json(
          source.value === 'both'
            ? toCompositeRiskCapital(selfScoped)
            : selfScoped,
        );
      },
    ),

    mock.get(
      '/v1/risk/{chain_id}/{token_address}/breakdown',
      async ({ params, query, response }) => {
        await mockDelay(LIST_DELAY_MS);
        const breakdown = breakdownFor(params.token_address);

        if (breakdown === undefined) {
          return response.untyped(
            problemResponse(
              notFound(
                `No collateral breakdown for ${params.token_address} on chain ${params.chain_id}`,
              ),
            ),
          );
        }

        const primeId = query.get('prime_id');
        const share = poolShareFor(primeId);
        if (share === undefined) {
          // The real endpoint cannot scale to a prime it holds no share data
          // for, and says so rather than serving the whole pool as one prime's.
          return response.untyped(
            problemResponse(unavailable('share_data_missing')),
          );
        }

        return response(200).json(scaleBreakdown(breakdown, share));
      },
    ),

    mock.get('/v1/risk/rrc', ({ query, response }) => {
      // `prime_id` is `required` in the document, so openapi-msw types this as
      // `string` — but the underlying URLSearchParams read is nullable, so the
      // check is a runtime one the types cannot make for us.
      const primeId: string | null = query.get('prime_id');
      if (primeId === null || primeId === '') {
        return response.untyped(
          problemResponse(invalidQueryParam('prime_id', 'field required')),
        );
      }

      const chainId = readChainId(query.get('chain_id'));
      if (!chainId.ok) {
        return response.untyped(problemResponse(chainId.problem));
      }
      const tokenAddress = query.get('token_address');
      const assetId = query.get('asset_id');

      // Exactly one identity: `asset_id`, or the `chain_id` + `token_address`
      // pair. Both forms or neither is a 422, and so is half a pair.
      const hasPair = chainId.value !== null && tokenAddress !== null;
      const hasPartialPair =
        !hasPair && (chainId.value !== null || tokenAddress !== null);
      if ((assetId !== null) === hasPair || hasPartialPair) {
        return response.untyped(
          problemResponse(
            invalidQueryParam(
              'token_address',
              'pass exactly one of asset_id or (chain_id, token_address)',
            ),
          ),
        );
      }

      const envelope = rrcEnvelope(primeId, chainId.value, tokenAddress);
      return envelope === undefined
        ? response.untyped(
            problemResponse(
              notFound(
                'Asset is not a known receipt token, or no models apply',
              ),
            ),
          )
        : response(200).json(envelope);
    }),
  ];
}
