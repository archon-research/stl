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

export type StarRiskCapitalRow = {
  star: string;
  exposure: string;
  total_rc: string;
  financial_rrc: string;
  exposure_share: string;
  risk_tolerance_ratio: string;
};

export type StarRiskCapitalResponse = {
  data?: {
    results?: StarRiskCapitalRow[];
  };
  status?: number;
  success?: boolean;
};
