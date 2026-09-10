/**
 * The held book as the two curation worksheets have it: 14 securities and the
 * entities they point at.
 *
 * Taken from the live worksheets (security classification, entity curation)
 * rather than invented, so the fixtures carry the real gaps — and there are
 * three kinds, each of which the UI has to have an answer for:
 *
 * 1. **A security with no issuer.** `sec-weth` is an issuer-less canonical
 *    wrapper, and `sec-gaclo1`'s issuer is unconfirmed. Both are rows the store
 *    accepts and `node_validity` flags, which is why the `security_issued`
 *    shape is EXPECTED and not REQUIRED.
 * 2. **A HAS_UNDERLYING edge whose target is not loaded.** `sec-weth`'s
 *    underlying is native ETH, which is not a node yet. The worksheet says
 *    "load sec-eth first or leave the edge pending", and the mock leaves it
 *    pending — so the endpoint-existence rejection (GQ-11) is reachable offline.
 * 3. **Subtype values with no concept to point at.** The worksheet's subtype
 *    dropdown offers SYNTHETIC, COMMODITY_BACKED, AAA_TRANCHE, GOV_SECURITIES
 *    and OTHER; the seeded `instrument_subtype` class contains none of them
 *    (33 concepts, and the nearest is `concept-sst-synthetic_cdo`). So five of
 *    nine offered subtypes cannot become a BELONGS_TO edge at all, and the
 *    securities below that need one carry no subtype membership. That is a
 *    vocabulary-to-taxonomy gap in the seed, not a modelling choice, and it is
 *    the sort of thing building the client first is supposed to surface.
 */

export type SecuritySeed = {
  id: string;
  attrs: Record<string, unknown>;
  chainId?: number;
  /** BELONGS_TO targets that exist in the seeded taxonomy. */
  belongsTo: readonly string[];
  issuerEntityId?: string;
  underlying?: string;
};

export type EntitySeed = {
  id: string;
  status: string;
  attrs: Record<string, unknown>;
  parentEntityId?: string;
};

const ERC20 = 'ERC-20';
const ERC4626 = 'ERC-4626';

export const SECURITY_SEED: readonly SecuritySeed[] = [
  {
    id: 'sec-usdc',
    chainId: 1,
    attrs: {
      ticker: 'USDC',
      security_name: 'USD Coin',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-stablecoin',
      'concept-sst-fiat_backed',
    ],
    issuerEntityId: 'em-issuer-circle',
  },
  {
    id: 'sec-usdt',
    chainId: 1,
    attrs: {
      ticker: 'USDT',
      security_name: 'Tether USD',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-stablecoin',
      'concept-sst-fiat_backed',
    ],
    issuerEntityId: 'em-issuer-tether',
  },
  {
    id: 'sec-pyusd',
    chainId: 1,
    attrs: {
      ticker: 'PYUSD',
      security_name: 'PayPal USD',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-stablecoin',
      'concept-sst-fiat_backed',
    ],
    issuerEntityId: 'em-issuer-paxos',
  },
  {
    id: 'sec-rlusd',
    chainId: 1,
    attrs: {
      ticker: 'RLUSD',
      security_name: 'Ripple USD',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-stablecoin',
      'concept-sst-fiat_backed',
    ],
    issuerEntityId: 'em-issuer-standard-custody',
  },
  {
    id: 'sec-ausd',
    chainId: 1,
    attrs: {
      ticker: 'AUSD',
      security_name: 'Agora Dollar',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-stablecoin',
      'concept-sst-fiat_backed',
    ],
    issuerEntityId: 'em-issuer-agora',
  },
  {
    id: 'sec-usds',
    chainId: 1,
    attrs: {
      ticker: 'USDS',
      security_name: 'Sky USDS',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-stablecoin',
      'concept-sst-crypto_backed',
    ],
    issuerEntityId: 'em-issuer-sky',
  },
  {
    id: 'sec-dai',
    chainId: 1,
    attrs: {
      ticker: 'DAI',
      security_name: 'Dai Stablecoin',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-stablecoin',
      'concept-sst-crypto_backed',
    ],
    issuerEntityId: 'em-issuer-sky',
  },
  {
    // SYNTHETIC has no concept node, so this one carries asset class and type
    // only — and shows up on the worklist for the subtype it cannot express.
    id: 'sec-usde',
    chainId: 1,
    attrs: {
      ticker: 'USDe',
      security_name: 'Ethena USDe',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-stablecoin',
    ],
    issuerEntityId: 'em-issuer-ethena',
  },
  {
    id: 'sec-susds',
    chainId: 1,
    attrs: {
      ticker: 'sUSDS',
      security_name: 'Savings USDS',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC4626,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-yield_bearing_token',
    ],
    issuerEntityId: 'em-issuer-sky',
    underlying: 'sec-usds',
  },
  {
    id: 'sec-susde',
    chainId: 1,
    attrs: {
      ticker: 'sUSDe',
      security_name: 'Staked USDe',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC4626,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-yield_bearing_token',
    ],
    issuerEntityId: 'em-issuer-ethena',
    underlying: 'sec-usde',
  },
  {
    // GOV_SECURITIES has no concept node either.
    id: 'sec-buidl',
    chainId: 1,
    attrs: {
      ticker: 'BUIDL-I',
      security_name: 'BlackRock USD Institutional Digital Liquidity Fund',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
    },
    belongsTo: [
      'concept-ac-money_market',
      'concept-st-money_market-tokenised_fund',
    ],
    issuerEntityId: 'em-issuer-blackrock-buidl',
  },
  {
    // Issuer unconfirmed in the worksheet: a Grove AAA CLO vehicle to verify
    // against the fund docs. Stored without one, flagged, out of metrics.
    id: 'sec-gaclo1',
    chainId: 43_114,
    attrs: {
      ticker: 'GACLO-1',
      security_name: 'Grove AAA CLO',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
      credit_tranche: 'AAA',
    },
    belongsTo: [
      'concept-ac-structured_credit',
      'concept-st-structured_credit-clo',
    ],
  },
  {
    id: 'sec-stac',
    chainId: 1,
    attrs: {
      ticker: 'STAC',
      security_name: 'Securitize AAA CLO',
      currency: 'USD',
      is_tokenised: true,
      token_standard: ERC20,
      credit_tranche: 'AAA',
    },
    belongsTo: [
      'concept-ac-structured_credit',
      'concept-st-structured_credit-clo',
    ],
    issuerEntityId: 'em-issuer-securitize-aaa-clo',
  },
  {
    // Issuer-less by nature, and its underlying (native ETH) is not a node yet.
    id: 'sec-weth',
    chainId: 1,
    attrs: {
      ticker: 'WETH',
      security_name: 'Wrapped Ether',
      currency: 'ETH',
      is_tokenised: true,
      token_standard: ERC20,
    },
    belongsTo: [
      'concept-ac-digital_asset',
      'concept-st-digital_asset-wrapped_token',
    ],
  },
];

export const ENTITY_SEED: readonly EntitySeed[] = [
  {
    id: 'em-prime-group',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Sky Prime Group',
      entity_type: 'UNKNOWN',
      counterparty_role: 'PRIME',
      is_internal: true,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
  },
  {
    id: 'em-prime-1',
    status: 'ACTIVE',
    attrs: {
      short_name: 'spark',
      entity_type: 'SPV',
      counterparty_role: 'PRIME',
      is_internal: true,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
    parentEntityId: 'em-prime-group',
  },
  {
    id: 'em-prime-2',
    status: 'ACTIVE',
    attrs: {
      short_name: 'grove',
      entity_type: 'SPV',
      counterparty_role: 'PRIME',
      is_internal: true,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
    parentEntityId: 'em-prime-group',
  },
  {
    id: 'em-prime-3',
    status: 'ACTIVE',
    attrs: {
      short_name: 'obex',
      entity_type: 'SPV',
      counterparty_role: 'PRIME',
      is_internal: true,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
    parentEntityId: 'em-prime-group',
  },
  {
    id: 'em-issuer-circle',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Circle',
      legal_name: 'Circle Internet Financial, LLC',
      entity_type: 'LLC',
      counterparty_role: 'ISSUER',
      is_internal: false,
      domicile_country: 'US',
      country_of_risk: 'US',
      sector: 'FINANCIALS',
    },
  },
  {
    id: 'em-issuer-tether',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Tether',
      legal_name: 'Tether Operations, S.A. de C.V.',
      entity_type: 'CORPORATION',
      counterparty_role: 'ISSUER',
      is_internal: false,
      domicile_country: 'SV',
      country_of_risk: 'SV',
      sector: 'FINANCIALS',
    },
  },
  {
    // A DAO with no LEI and an unverified Cayman wrapper: the entity_base shape
    // is satisfied, the alias register stays empty, and that is a legitimate
    // steady state rather than incomplete curation.
    id: 'em-issuer-sky',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Sky',
      entity_type: 'DAO',
      counterparty_role: 'ISSUER',
      is_internal: false,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
  },
  {
    id: 'em-issuer-ethena',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Ethena',
      legal_name: 'Ethena (BVI) Limited',
      entity_type: 'CORPORATION',
      counterparty_role: 'ISSUER',
      is_internal: false,
      domicile_country: 'VG',
      country_of_risk: 'VG',
      sector: 'UNKNOWN',
    },
  },
  {
    id: 'em-issuer-paxos',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Paxos',
      legal_name: 'Paxos Trust Company, LLC',
      entity_type: 'LLC',
      counterparty_role: 'ISSUER',
      is_internal: false,
      domicile_country: 'US',
      country_of_risk: 'US',
      sector: 'FINANCIALS',
    },
  },
  {
    id: 'em-issuer-agora',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Agora',
      entity_type: 'CORPORATION',
      counterparty_role: 'ISSUER',
      is_internal: false,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
  },
  {
    id: 'em-issuer-standard-custody',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Ripple (Standard Custody)',
      legal_name: 'Standard Custody & Trust Company, LLC',
      entity_type: 'TRUST',
      counterparty_role: 'ISSUER',
      is_internal: false,
      domicile_country: 'US',
      country_of_risk: 'US',
      sector: 'FINANCIALS',
    },
  },
  {
    id: 'em-issuer-blackrock-buidl',
    status: 'ACTIVE',
    attrs: {
      short_name: 'BlackRock BUIDL fund',
      legal_name: 'BlackRock USD Institutional Digital Liquidity Fund Ltd',
      entity_type: 'FUND',
      counterparty_role: 'ISSUER',
      is_internal: false,
      domicile_country: 'VG',
      country_of_risk: 'VG',
      sector: 'FINANCIALS',
    },
  },
  {
    id: 'em-issuer-securitize-aaa-clo',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Securitize AAA CLO',
      entity_type: 'SPV',
      counterparty_role: 'ISSUER',
      is_internal: false,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
  },
  {
    id: 'em-coinbase',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Coinbase (group apex)',
      legal_name: 'Coinbase Global, Inc.',
      entity_type: 'CORPORATION',
      is_internal: false,
      domicile_country: 'US',
      country_of_risk: 'US',
      sector: 'FINANCIALS',
    },
  },
  {
    id: 'em-issuer-coinbase',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Coinbase, Inc.',
      legal_name: 'Coinbase, Inc.',
      entity_type: 'CORPORATION',
      counterparty_role: 'ISSUER',
      is_internal: false,
      domicile_country: 'US',
      country_of_risk: 'US',
      sector: 'FINANCIALS',
    },
    parentEntityId: 'em-coinbase',
  },
  {
    id: 'em-custodian-coinbase',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Coinbase Custody Trust',
      legal_name: 'Coinbase Custody Trust Company, LLC',
      entity_type: 'TRUST',
      counterparty_role: 'CUSTODIAN',
      is_internal: false,
      domicile_country: 'US',
      country_of_risk: 'US',
      sector: 'FINANCIALS',
    },
    parentEntityId: 'em-coinbase',
  },
  {
    id: 'em-aave-group',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Aave Group SEZC',
      legal_name: 'Aave Group SEZC',
      entity_type: 'CORPORATION',
      counterparty_role: 'PROTOCOL_OPERATOR',
      is_internal: false,
      domicile_country: 'KY',
      country_of_risk: 'KY',
      sector: 'UNKNOWN',
    },
  },
  {
    id: 'em-protocol-3',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Aave V3',
      entity_type: 'UNKNOWN',
      counterparty_role: 'PROTOCOL',
      is_internal: false,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
    parentEntityId: 'em-aave-group',
  },
  {
    id: 'em-morpho',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Morpho Labs',
      entity_type: 'UNKNOWN',
      counterparty_role: 'PROTOCOL_OPERATOR',
      is_internal: false,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
  },
  {
    id: 'em-protocol-6',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Morpho Blue',
      entity_type: 'UNKNOWN',
      counterparty_role: 'PROTOCOL',
      is_internal: false,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
    parentEntityId: 'em-morpho',
  },
  {
    // LEI REJECTED in the worksheet: the GLEIF hit is an unrelated Irish
    // company sharing the name. Kept as a seeded entity with no alias.
    id: 'em-maple',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Maple Finance',
      entity_type: 'UNKNOWN',
      counterparty_role: 'PROTOCOL_OPERATOR',
      is_internal: false,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
  },
  {
    id: 'em-phoenix-labs',
    status: 'ACTIVE',
    attrs: {
      short_name: 'Phoenix Labs',
      entity_type: 'UNKNOWN',
      counterparty_role: 'PROTOCOL_OPERATOR',
      is_internal: false,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
  },
  {
    id: 'em-protocol-1',
    status: 'ACTIVE',
    attrs: {
      short_name: 'SparkLend',
      entity_type: 'UNKNOWN',
      counterparty_role: 'PROTOCOL',
      is_internal: false,
      domicile_country: 'XX',
      country_of_risk: 'XX',
      sector: 'UNKNOWN',
    },
    parentEntityId: 'em-phoenix-labs',
  },
];

/** Sources, so the one REQUIRED-severity shape has something to act on. */
export const SOURCE_SEED: readonly EntitySeed[] = [
  {
    id: 'src-gleif',
    status: 'ACTIVE',
    attrs: {
      label: 'GLEIF',
      licence: 'CC0 1.0',
      redistributable: true,
      reliability: 'AUTHORITATIVE',
    },
  },
  {
    id: 'src-worksheet',
    status: 'ACTIVE',
    attrs: {
      label: 'Curation worksheet',
      licence: 'internal',
      redistributable: false,
      reliability: 'CORROBORATING',
    },
  },
];
