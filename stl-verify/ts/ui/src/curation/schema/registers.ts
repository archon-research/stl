import * as z from 'zod';

import {
  chainId,
  evmAddress,
  lei,
  nodeId,
  anyNodeId,
  ui,
  validDate,
  validToDate,
} from './primitives.ts';
import { checkProvenance, provenanceInput } from './provenance.ts';

/**
 * The two registers (VEC-616, deferred to their own wave).
 *
 * They are the reason "everything is an edge" is not quite true. Both carry a
 * uniqueness guarantee that an append-only edge store cannot: exactly one
 * current mapping per instrument key, and per-scheme uniqueness per alias. The
 * hottest join in the system — position `instrument_key` to security — depends
 * on that guarantee, so the registers stay separate tables with their own
 * current-row constraint.
 *
 * For the UI that difference is visible in one place and it matters: re-pointing
 * a register entry is a *close-and-open pair*, not a single write. The form for
 * it is therefore not a generated create form but a workflow (`workflows.ts`),
 * and the reason code is REPOINT, which requires approval.
 */

/** `instrument_register.key_namespace`, the namespaces wave 1 names. */
const KEY_NAMESPACES = [
  'evm_contract',
  'morpho_market',
  'registry_ilk',
  'provider_package',
  'native_asset',
] as const;

/** `alias_register.id_scheme`. Public identifiers, never used as ids. */
const ID_SCHEMES = [
  'ISIN',
  'FIGI',
  'CUSIP',
  'LEI',
  'TICKER',
  'HOLDER_ADDRESS',
] as const;

export const instrumentRegisterWrite = z
  .object({
    instrument_key: ui(z.string().trim().min(3).max(256), {
      label: 'Instrument key',
      help: 'The instrument’s own namespaced key. Never carries a house classifier — position_id hashes it',
      group: 'Key',
      order: 1,
    }),
    key_namespace: ui(z.enum(KEY_NAMESPACES), {
      label: 'Namespace',
      group: 'Key',
      order: 2,
    }),
    security_id: ui(nodeId('SECURITY'), {
      widget: 'reference',
      label: 'Resolves to security',
      help: 'Many keys may resolve to one security; exactly one current row per key',
      group: 'Key',
      order: 3,
      targetKinds: ['SECURITY'],
    }),
    chain_id: ui(chainId.optional(), {
      label: 'Chain',
      group: 'Key',
      order: 4,
    }),
    address: ui(evmAddress.optional(), {
      label: 'Address',
      help: 'Where the namespace is evm_contract, the key and the address agree',
      group: 'Key',
      order: 5,
    }),
    decimals: ui(z.int().min(0).max(36).optional(), {
      label: 'Decimals',
      group: 'Key',
      order: 6,
    }),
    valid_from: ui(validDate, {
      label: 'Valid from',
      group: 'Lifecycle',
      order: 1,
    }),
    ...provenanceInput.shape,
  })
  .check((ctx) => {
    const v = ctx.value;
    checkProvenance(v, ctx);

    // The key and the address are two spellings of one fact in the evm case, and
    // a mismatch there re-points the hottest join at the wrong security while
    // looking correct in both columns on their own.
    if (v.key_namespace === 'evm_contract') {
      if (v.address === undefined) {
        ctx.issues.push({
          code: 'custom',
          path: ['address'],
          message: 'an evm_contract key is an address; record it',
          input: v.address,
        });
      } else if (
        !v.instrument_key.toLowerCase().includes(v.address.toLowerCase())
      ) {
        ctx.issues.push({
          code: 'custom',
          path: ['instrument_key'],
          message: 'the key and the address disagree',
          input: v.instrument_key,
        });
      }

      if (v.chain_id === undefined) {
        ctx.issues.push({
          code: 'custom',
          path: ['chain_id'],
          message: 'the same address on two chains is two instruments',
          input: v.chain_id,
        });
      }
    }
  });

export const aliasRegisterWrite = z
  .object({
    id_scheme: ui(z.enum(ID_SCHEMES), {
      label: 'Scheme',
      group: 'Alias',
      order: 1,
    }),
    id_value: ui(z.string().trim().min(1).max(256), {
      label: 'Value',
      help: 'The public identifier. An alias is a lookup, never load-bearing',
      group: 'Alias',
      order: 2,
    }),
    node_id: ui(anyNodeId, {
      widget: 'reference',
      label: 'Resolves to node',
      group: 'Alias',
      order: 3,
    }),
    valid_from: ui(validDate, {
      label: 'Valid from',
      group: 'Lifecycle',
      order: 1,
    }),
    valid_to: ui(validToDate.default('infinity'), {
      label: 'Valid to',
      group: 'Lifecycle',
      order: 2,
    }),
    ...provenanceInput.shape,
  })
  .check((ctx) => {
    const v = ctx.value;
    checkProvenance(v, ctx);

    // Each scheme has a shape, and a wrong-shaped alias resolves to nothing
    // rather than resolving wrongly — which makes it invisible until a join
    // comes up short.
    if (v.id_scheme === 'LEI' && !lei.safeParse(v.id_value).success) {
      ctx.issues.push({
        code: 'custom',
        path: ['id_value'],
        message: 'an LEI is 18 alphanumerics then 2 check digits',
        input: v.id_value,
      });
    }

    if (v.id_scheme === 'ISIN' && !/^[A-Z]{2}[A-Z0-9]{9}\d$/.test(v.id_value)) {
      ctx.issues.push({
        code: 'custom',
        path: ['id_value'],
        message:
          'an ISIN is a 2-letter country, 9 alphanumerics and a check digit',
        input: v.id_value,
      });
    }

    if (
      v.id_scheme === 'HOLDER_ADDRESS' &&
      !evmAddress.safeParse(v.id_value).success
    ) {
      ctx.issues.push({
        code: 'custom',
        path: ['id_value'],
        message: 'a holder address is a 20-byte hex address',
        input: v.id_value,
      });
    }
  });
