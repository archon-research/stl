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
