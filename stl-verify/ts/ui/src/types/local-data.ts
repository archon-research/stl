export type LocalChainRow = {
  chain_id: number;
  name: string;
};

export type LocalProtocolRow = {
  id: number;
  chain_id: number;
  encode: string;
  name: string;
};

export type LocalCostRow = {
  service: string;
  total_costs_usd: number | null;
  line_items: Record<string, number | null>;
};
