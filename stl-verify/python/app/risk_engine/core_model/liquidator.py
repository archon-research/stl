# ============================================================
# Liquidator: A class to analyze borrower positions and liquidation risks

# This class provides tools to compute the bad debt exposure for each scenario
# of the simulated price paths.
# Swap fee --> Fixed
# Gas fee --> Fixed
# Slippage --> CEXs based or Hyperlend based or Uniswap V3-based
# ============================================================

import numpy as np
import pandas as pd

from app.risk_engine.core_model import importer


class Liquidator:
    def __init__(self, borrowers_df: pd.DataFrame, market_df: pd.DataFrame, *, seed: int = 0) -> None:
        """Initializes the Liquidator with borrower and market data.
        Compute for every modeled collateral its sell orderbook."""

        # New user final DataFrame after cleaning
        users_final = borrowers_df.copy()
        market_final = market_df.copy()

        LT = users_final["lltv"].reset_index(drop=True)
        LTV = users_final["ltv"].reset_index(drop=True)
        HF = users_final["health_factor"].reset_index(drop=True)
        LB = users_final["liquidation_incentive"].reset_index(drop=True)
        tokens = market_df["token_symbol"].to_list()
        oracle_prices_dict = market_final.set_index("token_symbol")["oracle_price"].to_dict()

        all_sell_orderbooks = importer.load_orderbook_data(tokens)

        self.HF = HF
        self.LT = LT
        self.LTV = LTV
        self.LB = LB
        self.seed = seed
        self.user_df = users_final
        self.oracle_prices = oracle_prices_dict
        self.sell_orderbooks = all_sell_orderbooks

    @staticmethod
    def slippage_calculator(ticks_df: pd.DataFrame, amount_liq_usd: np.ndarray, sim_price: float) -> np.ndarray:

        N = amount_liq_usd.shape[0]
        add_slippage = np.zeros(N)

        prices = ticks_df["price"].to_numpy(dtype=np.float64)
        liquidity = ticks_df["liquidity"].to_numpy(dtype=np.float64)

        mask = prices <= sim_price
        prices = prices[mask]
        liquidity = liquidity[mask]

        cum_liq = np.cumsum(liquidity)
        cum_value = np.cumsum(liquidity * prices)

        if prices.size == 0:
            return np.ones_like(amount_liq_usd)

        available_liq = cum_liq[-1]

        overflow = amount_liq_usd > available_liq
        add_slippage[overflow] = (amount_liq_usd[overflow] - available_liq) / amount_liq_usd[overflow]

        liq_eff = np.minimum(amount_liq_usd, available_liq)

        idx = np.minimum(np.searchsorted(cum_liq, liq_eff, side="left"), len(cum_liq) - 1)
        value_used = cum_value[idx]
        liq_used = cum_liq[idx]
        avg_price = value_used / liq_used

        price_impact = (sim_price - avg_price) / sim_price
        slippage = np.minimum(price_impact + add_slippage, 1.0)

        slippage[amount_liq_usd == 0.0] = 0.0

        return slippage

    @staticmethod
    def slippage_calculator_cum(
        ticks_df: pd.DataFrame, amount_liq_usd: np.ndarray, sim_price: float, already_consumed: float = 0.0
    ) -> np.ndarray:

        N = amount_liq_usd.shape[0]
        add_slippage = np.zeros(N)

        prices = ticks_df["price"].to_numpy(dtype=np.float64)
        liquidity = ticks_df["liquidity"].to_numpy(dtype=np.float64)

        mask = prices <= sim_price
        prices = prices[mask]
        liquidity = liquidity[mask]

        if prices.size == 0:
            return np.ones_like(amount_liq_usd)

        cum_liq = np.cumsum(liquidity)
        cum_value = np.cumsum(liquidity * prices)

        total_available = cum_liq[-1]
        effective_avail = max(total_available - already_consumed, 0.0)

        # Overflow: amount exceeds what remains in the book after prior consumption
        overflow = amount_liq_usd > effective_avail
        add_slippage[overflow] = (amount_liq_usd[overflow] - effective_avail) / amount_liq_usd[overflow]

        liq_eff = np.minimum(amount_liq_usd, effective_avail)
        total_needed = already_consumed + liq_eff  # absolute position in book

        idx_end = np.minimum(np.searchsorted(cum_liq, total_needed, side="left"), len(cum_liq) - 1)
        idx_base = np.minimum(np.searchsorted(cum_liq, np.full(N, already_consumed), side="left"), len(cum_liq) - 1)

        value_used = cum_value[idx_end] - cum_value[idx_base]
        liq_used = cum_liq[idx_end] - cum_liq[idx_base]

        safe_liq = np.where(liq_used > 0, liq_used, 1e-12)
        avg_price = value_used / safe_liq

        price_impact = (sim_price - avg_price) / sim_price
        slippage = np.minimum(price_impact + add_slippage, 0.9999)
        slippage[amount_liq_usd == 0.0] = 0.0

        return slippage

    @staticmethod
    def margin_call_function(
        debt: np.ndarray,
        collateral_value: np.ndarray,
        margin_band_mask: np.ndarray,
        margin_call_ltv: np.ndarray,
        rng: np.random.Generator,
        probability: float = 0.8,
    ) -> np.ndarray:
        """
        Fast margin-call collateral addition.
        Only acts on pre-filtered margin-band wallets.
        """

        add_collateral = np.zeros_like(debt)

        idx = np.flatnonzero(margin_band_mask)
        if idx.size == 0:
            return add_collateral

        u = rng.random(idx.size)
        repay = u < probability
        active_idx = idx[repay]

        add = (debt / margin_call_ltv) - collateral_value
        add_collateral[active_idx] = np.maximum(add[active_idx], 0.0)

        # print("\nMargin call function invoked.")
        # print(f"Current ltvs: {debt / collateral_value}")
        # print(f"Collateral Value: {collateral_value}")

        # print(f"Indices: {idx}")
        # print(f"Random uniforms: {u}")
        # print(f"Repay mask: {repay}")
        # print(f"Active indexes: {active_idx}")
        # print(f"Add collateral: {add_collateral}")

        return add_collateral

    @staticmethod
    def get_delta(cols, default_delta=pd.Timedelta(hours=1)):
        if len(cols) > 1:
            return cols[1] - cols[0]

        if hasattr(cols, "freq") and cols.freq is not None:
            return cols.freq

        inferred = pd.infer_freq(cols)
        if inferred is not None:
            return pd.Timedelta(inferred)

        return default_delta

    @staticmethod
    def compute_run_stats(
        summary_df: pd.DataFrame,
        tot_debt: float | None,
        perc: float = 0.975,
        users_df: pd.DataFrame | None = None,
    ) -> dict:
        """
        Compute summary statistics for a completed simulation run.

        Parameters
        ----------
        summary_df : DataFrame returned by simulate_liquidations()
        tot_debt   : total loan exposure in USD (denominator for CRR)
        perc       : confidence level for VaR / ES (e.g. 0.975 = 97.5th pct)
        users_df   : borrower-level DataFrame — used to compute concentration metrics
                     (HHI, top-1 share). Optional; metrics are None if not provided.

        Returns
        -------
        dict with keys:
            n_scenarios         : int   — total number of MC scenarios
            prob_no_bad_debt    : float — fraction of scenarios with zero bad debt (0–1)
            prob_bad_debt       : float — fraction of scenarios with any bad debt (0–1)
            max_bad_debt_usd    : float — worst-case bad debt across all scenarios
            max_bad_debt_pct    : float | None — max_bad_debt_usd / tot_debt * 100
            mean_bad_debt_usd   : float | None — mean bad debt conditional on bad_debt > 0
            mean_crr            : float | None — EL = E[bad_debt] / tot_debt * 100 (Basel EL)
            crr_var             : float | None — CRR VaR at confidence level (%)
            crr_es              : float | None — CRR ES  at confidence level (%)
            es_el_ratio         : float | None — crr_es / mean_crr (concentration diagnostic)
            hhi                 : float | None — Herfindahl-Hirschman Index of borrower
                                                 exposures (0=granular, 1=single borrower)
            top1_share          : float | None — largest single borrower's share of tot_debt
            n_borrowers         : int   | None — number of active borrowers
        """
        import numpy as np

        _empty = {
            "n_scenarios": 0,
            "prob_no_bad_debt": None,
            "prob_bad_debt": None,
            "max_bad_debt_usd": None,
            "max_bad_debt_pct": None,
            "mean_bad_debt_usd": None,
            "mean_crr": None,
            "crr_var": None,
            "crr_es": None,
            "es_el_ratio": None,
            "hhi": None,
            "top1_share": None,
            "n_borrowers": None,
        }

        if summary_df is None or summary_df.empty:
            return _empty

        bad_debt = summary_df["net_bad_debt_total"]
        n = len(bad_debt)

        prob_no_bad_debt = float((bad_debt == 0).mean())
        prob_bad_debt = 1.0 - prob_no_bad_debt
        max_bad_debt_usd = float(bad_debt.max())

        tail_vals = bad_debt[bad_debt > 0]
        mean_bad_debt_usd = float(tail_vals.mean()) if len(tail_vals) > 0 else None

        max_bad_debt_pct = (
            round(max_bad_debt_usd / tot_debt * 100, 6) if (tot_debt is not None and tot_debt > 0) else None
        )

        crr_var = crr_es = mean_crr = es_el_ratio = None
        if tot_debt is not None and tot_debt > 0:
            crr = bad_debt / tot_debt * 100
            crr_var = round(float(np.quantile(crr, perc, method="inverted_cdf")), 6)
            n_tail = max(1, int(np.ceil((1 - perc) * n)))
            crr_es = round(float(crr.nlargest(n_tail).mean()), 6)
            mean_crr = round(float(bad_debt.mean()) / tot_debt * 100, 6)
            if mean_crr and mean_crr > 0:
                es_el_ratio = round(crr_es / mean_crr, 1)

        # ── Concentration metrics from borrower-level data ────────────────────
        hhi = top1_share = n_borrowers = None
        _CRR_HHI_K: float = 1.0
        if users_df is not None and not users_df.empty and "total_borrow_usd" in users_df.columns:
            borrow = users_df["total_borrow_usd"].fillna(0)
            active = borrow[borrow > 0]
            n_borrowers = int(len(active))
            if n_borrowers > 0 and active.sum() > 0:
                shares = active / active.sum()
                hhi = round(float((shares**2).sum()), 6)
                top1_share = round(float(shares.max()), 6)

        # ── Concentration-adjusted CRR ────────────────────────────────────────
        # CRR_adj = EL × (1 + k × HHI)
        #   HHI = 0  (granular)     : CRR_adj = EL
        #   HHI = 1  (single name)  : CRR_adj = EL × (1 + k)   e.g. 2×EL when k=1
        crr_adj = None
        if mean_crr is not None:
            _w = hhi if hhi is not None else 0.0
            crr_adj = round(mean_crr * (1 + _CRR_HHI_K * _w), 6)

        return {
            "n_scenarios": n,
            "prob_no_bad_debt": prob_no_bad_debt,
            "prob_bad_debt": prob_bad_debt,
            "max_bad_debt_usd": max_bad_debt_usd,
            "max_bad_debt_pct": max_bad_debt_pct,
            "mean_bad_debt_usd": mean_bad_debt_usd,
            "mean_crr": mean_crr,
            "crr_adj": crr_adj,
            "crr_var": crr_var,
            "crr_es": crr_es,
            "es_el_ratio": es_el_ratio,
            "hhi": hhi,
            "top1_share": top1_share,
            "n_borrowers": n_borrowers,
        }

    def simulate_liquidations(
        self,
        all_prices: dict,
        product: str,
        *,
        swap_fee: float = 0.0,
        gas_fee_usd: float = 0.0,
        perc: float = 0.995,
        protection_usd: float = 0.0,
        margin_call_trigger: float = 0.05,
        margin_call_target_ltv: float | None = None,
        margin_call_cure_prob: float = 0.8,
    ) -> dict:
        """
        margin_call_trigger      : pp buffer below LT that triggers a margin call.
                                   A position triggers when LTV >= LT - margin_call_trigger.
                                   Default 0.05 means margin call fires at LTV >= LT - 5pp.
        margin_call_target_ltv   : LTV the borrower restores to on cure.
                                   None (default) → restore to the borrower's initial LTV.
                                   A float (e.g. LT - 0.15) → restore to that fixed target.
        margin_call_cure_prob    : probability that a borrower in the margin-call zone
                                   actually cures (posts collateral). Default 0.8.
        """
        """
        Profit-aware simulation with 'count bad debt once' semantics.

        Bad debt accounting:
        - On the FIRST step a wallet is unsafe and not executed, record EAD = R_req (once) and mark it defaulted.
        - Later, if a defaulted wallet is executed, treat R as a recovery up to outstanding EAD.
        - Report: EAD_total, recoveries_total, net_bad_debt_total = EAD_total - recoveries_total.

        Liquidation rule (same as before):
        - Must bring LTV to exactly liq_threshold in one shot (R_req).
        - Execute only if feasible and profitable (after swap fee, slippage, and gas).
        - Slippage scales with R_req relative to max position USD at that step, capped at max_slippage.

        Returns:
        result['summary']: scenario-level aggregates with:
            ['bad_debt_ead_total','recoveries_total','net_bad_debt_total',
            'debt_repaid_total','collateral_liquidated_total',
            'final_total_debt','final_total_collateral']
        result['per_step'] (optional): (scenario, step) with
            ['new_ead_step','recoveries_step','net_bad_debt_outstanding_step',
            'debt_repaid_step','collateral_liquidated_step','executions_step']
        """
        users_final = self.user_df.copy().reset_index(drop=True)
        users_final = users_final.set_index("wallet_address")

        liq_bonus = users_final["liquidation_incentive"]
        first_token_prices = next(iter(all_prices.values()))
        N_SCEN, N_FORECAST = first_token_prices.shape

        all_prices = {k.upper(): v for k, v in all_prices.items()}
        modeled_tokens = set(all_prices.keys())

        for token, prices in all_prices.items():
            if prices is None or prices.empty:
                continue

            oracle = self.oracle_prices[token]

            cols = prices.columns

            delta = Liquidator.get_delta(prices.columns)

            t0 = cols.min() - delta
            t0_col = pd.DataFrame(oracle, index=prices.index, columns=[t0])

            prices = pd.concat([t0_col, prices], axis=1)
            prices = prices.sort_index(axis=1)

            all_prices[token] = prices

        def _base_token(col: str, suffix: str) -> str:
            return col.replace(suffix, "").upper()

        supply_cols = [c for c in users_final.columns if c.endswith("_supply") and "total_supply" not in c]
        borrow_cols = [c for c in users_final.columns if c.endswith("_borrow") and "total_borrow" not in c]
        borrow_usd_cols = [f"{c}_usd" for c in borrow_cols]
        TOT_DEBT = users_final[borrow_usd_cols].fillna(0).sum(axis=1).sum(axis=0)
        # print(f"TOT DEBT: {TOT_DEBT}")

        supply_tokens = {c: _base_token(c, "_supply") for c in supply_cols}  # col -> token
        borrow_tokens = {c: _base_token(c, "_borrow") for c in borrow_cols}

        # Modeled columns (we have price paths for these)
        mod_supply_cols = [c for c in supply_cols if supply_tokens[c] in modeled_tokens]
        mod_borrow_cols = [c for c in borrow_cols if borrow_tokens[c] in modeled_tokens]

        # Unmodeled columns (static in USD — no price path)
        unmod_supply_cols = [c for c in supply_cols if supply_tokens[c] not in modeled_tokens]
        unmod_borrow_cols = [c for c in borrow_cols if borrow_tokens[c] not in modeled_tokens]

        unmod_supply_usd_cols = [f"{c}_usd" for c in unmod_supply_cols]
        unmod_borrow_usd_cols = [f"{c}_usd" for c in unmod_borrow_cols]

        # Static unmodeled totals per wallet  (N_wallets,)
        unmod_supply_vec = users_final[unmod_supply_usd_cols].fillna(0).sum(axis=1).values  # (W,)
        unmod_borrow_vec = users_final[unmod_borrow_usd_cols].fillna(0).sum(axis=1).values

        N_BORROW = users_final.shape[0]
        T = N_FORECAST + 1

        supply_usd_tensor = np.zeros((N_BORROW, N_SCEN, T))
        borrow_usd_tensor = np.zeros((N_BORROW, N_SCEN, T))
        for col in mod_supply_cols:
            token = supply_tokens[col].upper()
            qty = users_final[col].fillna(0).values[:, None, None]  # (W,1,1)
            prices = all_prices[token].values[None, :, :]  # (1,S,T)

            supply_usd_tensor += qty * prices

        for col in mod_borrow_cols:
            token = borrow_tokens[col].upper()
            qty = users_final[col].fillna(0).values[:, None, None]
            prices = all_prices[token].values[None, :, :]

            borrow_usd_tensor += qty * prices

        supply_usd_tensor += unmod_supply_vec[:, None, None]
        borrow_usd_tensor += unmod_borrow_vec[:, None, None]

        one_plus_bonus = np.array(self.LB, dtype=np.float64)
        denom = -1.0 + self.LT.values * one_plus_bonus

        if np.any(denom >= 0):
            raise ValueError("Invalid params: -1 + LT*(1+bonus) must be < 0.")

        # ── Margin call setup ─────────────────────────────────────────────────
        # Applies to SYRUP (Maple) and ANCHORAGE only.
        # A margin call fires when a borrower's LTV enters the zone [LT - trigger, LT).
        # The borrower then has a `cure_prob` chance of posting additional collateral
        # to restore their LTV to the target. Each borrower can self-cure at most once
        # per scenario (further breaches are treated as defaults).
        _MC_PROTOCOLS = {"SYRUP", "ANCHORAGE", "MAPLE"}
        use_margin_call = any(p in product.upper() for p in _MC_PROTOCOLS)

        LT_arr = self.LT.values  # shape (N_BORROW,)
        LTV_init = np.array(self.LTV, dtype=np.float64)  # initial LTV per borrower

        # Target LTV on cure: explicit value or fall back to borrower's initial LTV
        if margin_call_target_ltv is not None:
            mc_target = np.full(N_BORROW, margin_call_target_ltv, dtype=np.float64)
        else:
            mc_target = LTV_init.copy()

        # Trigger threshold: LTV >= LT - margin_call_trigger
        mc_trigger_ltv = LT_arr - margin_call_trigger  # shape (N_BORROW,)

        debt_repaid_totals = np.zeros(N_SCEN)
        collat_liq_totals = np.zeros(N_SCEN)
        ead_totals = np.zeros(N_SCEN)  # sum of first defaults
        recoveries_totals = np.zeros(N_SCEN)
        final_debt_totals = np.zeros(N_SCEN)
        # TODO(bug#2): final_collat_totals is never populated -- always zero.
        # Impact: none on CRR or any stored metric. final_total_collateral is
        # included in the intermediate 'summary' DataFrame but excluded from
        # 'summary_df' before any computation; it is never read downstream.
        # The assignment is commented out below and `q` is undefined in that scope.
        final_collat_totals = np.zeros(N_SCEN)
        max_delta_ltv = np.zeros(N_SCEN)
        max_pct_loss = np.zeros(N_SCEN)
        pct_user_liq = np.zeros(N_SCEN)
        pct_user_default = np.zeros(N_SCEN)

        for s in range(N_SCEN):
            # Default tracking
            defaulted = np.zeros(N_BORROW, dtype=bool)
            ead_outstanding = np.zeros(N_BORROW, dtype=np.float64)  # per-wallet EAD not yet recovered
            each_user_loss = np.zeros(N_BORROW, dtype=np.float64)
            ever_liquidated = np.zeros(N_BORROW, dtype=bool)

            consumed_per_token = {token: 0.0 for token in modeled_tokens}

            scen_repaid = 0.0
            scen_colliq = 0.0
            scen_ead = 0.0
            scen_recv = 0.0
            n_users_defaulting = 0

            ltv_list = [0.0]
            D_adj = np.zeros(N_BORROW)
            CV_adj = np.zeros(N_BORROW)

            # Each borrower gets at most one self-cure per scenario.
            # After curing, further LT breaches are treated as real defaults.
            margin_called = np.zeros(N_BORROW, dtype=bool)

            # Seeded RNG for margin-call cure draws — reproducible per scenario.
            mc_rng = np.random.default_rng(self.seed + s)

            for t in range(T):
                D = borrow_usd_tensor[:, s, t] + D_adj
                CV = supply_usd_tensor[:, s, t] + CV_adj

                # ── Margin call (SYRUP / ANCHORAGE only) ─────────────────────
                # Fires before the liquidation check. Borrowers in the margin
                # band [LT - trigger, LT) who have not yet cured this scenario
                # have a `cure_prob` chance of posting collateral to restore
                # their LTV to mc_target.
                if use_margin_call:
                    current_ltv = D / np.maximum(CV, 1e-12)
                    in_band = (
                        (current_ltv >= mc_trigger_ltv)  # LTV entered the warning zone
                        & (current_ltv < LT_arr)  # not yet breached LT
                        & (~margin_called)  # hasn't already cured
                    )
                    if in_band.any():
                        added = Liquidator.margin_call_function(
                            debt=D,
                            collateral_value=CV,
                            margin_band_mask=in_band,
                            margin_call_ltv=mc_target,
                            rng=mc_rng,
                            probability=margin_call_cure_prob,
                        )
                        # Mark borrowers who actually posted collateral as cured
                        cured = in_band & (added > 0)
                        CV_adj[cured] += added[cured]
                        margin_called[cured] = True
                        # Recompute CV after cure before the liquidation check
                        CV = supply_usd_tensor[:, s, t] + CV_adj

                hf = (CV * self.LT.values) / np.maximum(D, 1e-12)
                unsafe = hf < 1.0

                if unsafe.any():
                    ltv_list.append((self.LT[unsafe] - self.LTV[unsafe]).max())

                if "AAVE" in product.upper() or "SPARK" in product.upper():
                    # Close factor is applied to debt (amount the liquidator repays).
                    # Seized collateral = R_req × (1 + bonus), handled via one_plus_bonus downstream.
                    close_factor = np.where(hf < 0.95, 1.0, 0.5)
                    R_req = close_factor * D
                else:
                    R_req = (self.LT * CV - D) / denom

                R_req = np.where(unsafe, np.maximum(R_req, 0.0), 0.0)
                R_req = np.minimum(R_req, D)

                R_cap_collat = CV / one_plus_bonus
                R_req = np.minimum(R_req, R_cap_collat)

                feasible = unsafe & (R_req > 0) & (R_req <= R_cap_collat)

                # do a for cicle to search for the best slippage opportunity
                best_profit = np.full(N_BORROW, -np.inf)
                best_price = np.zeros(N_BORROW)
                best_token_arr = np.full(N_BORROW, "", dtype=object)

                if any(unsafe):
                    for token in modeled_tokens:
                        P = all_prices[token].values[s, t]

                        slippage = Liquidator.slippage_calculator_cum(
                            self.sell_orderbooks[token], R_req, P, already_consumed=consumed_per_token[token]
                        )
                        # slippage = Liquidator.slippage_calculator(
                        #     self.sell_orderbooks[token],
                        #     R_req,
                        #     P
                        # )

                        proceeds = (1.0 - swap_fee - slippage) * one_plus_bonus * R_req
                        profit = proceeds - R_req - gas_fee_usd

                        better = profit > best_profit
                        best_profit[better] = profit[better]
                        best_price[better] = P
                        best_token_arr[better] = token

                    profit = best_profit
                else:
                    profit = 0.0

                profitable = profit >= 0.0
                do_exec = feasible & profitable
                for token in modeled_tokens:
                    token_mask = do_exec & (best_token_arr == token)
                    # Liquidators seize collateral worth R_req × (1 + bonus), not R_req.
                    # The order book is denominated in collateral USD, so we must consume
                    # the seized collateral amount — not the debt repaid — to correctly
                    # track cumulative book depletion across sequential liquidations.
                    consumed_per_token[token] += float(np.sum(R_req[token_mask] * one_plus_bonus[token_mask]))

                ever_liquidated |= do_exec

                # Execute liquidations
                R = np.where(do_exec, R_req, 0.0)
                seized_collat_usd = one_plus_bonus * R
                each_user_loss += liq_bonus * R

                D_adj -= R
                CV_adj -= seized_collat_usd

                step_repaid = float(np.sum(R))
                step_colliq = float(np.sum(seized_collat_usd))

                # ---- EAD once, recoveries later ----
                # New defaults this step (unsafe, not executed, and not already defaulted)
                new_default_mask = unsafe & (~do_exec) & (~defaulted)
                n_users_defaulting += int(new_default_mask.sum())
                # review the default probability integration here later
                new_ead = np.zeros(N_BORROW, dtype=np.float64)
                new_ead[new_default_mask] = R_req[new_default_mask]  # count once
                defaulted[new_default_mask] = True
                ead_outstanding[new_default_mask] += new_ead[new_default_mask]
                step_new_ead = float(np.sum(new_ead))
                scen_ead += step_new_ead
                # print(f"Scen EAD: {scen_ead}")

                # Recoveries from liquidations of already-defaulted wallets
                recov_mask = do_exec & defaulted
                # Recovery is the debt actually repaid, capped by outstanding EAD
                recov_amt = np.minimum(R[recov_mask], ead_outstanding[recov_mask])
                ead_outstanding[recov_mask] -= recov_amt
                step_recov = float(np.sum(recov_amt))
                scen_recv += step_recov

                scen_repaid += step_repaid
                scen_colliq += step_colliq

            den = np.where(D > 0, D, np.nan)

            max_pct_loss[s] = float(np.nanmax(each_user_loss / den))
            debt_repaid_totals[s] = scen_repaid
            collat_liq_totals[s] = scen_colliq
            ead_totals[s] = scen_ead
            recoveries_totals[s] = scen_recv
            final_debt_totals[s] = float(np.sum(D - R))
            # final_collat_totals[s]  = float(np.sum(q))
            max_delta_ltv[s] = max(ltv_list)
            pct_user_liq[s] = int(ever_liquidated.sum()) / N_BORROW
            pct_user_default[s] = n_users_defaulting / N_BORROW

        gross_bad_debt_total = np.maximum(0.0, ead_totals - recoveries_totals)

        # ── Apply defense mechanisms (FLC, subsidies, etc.) ───────────────────
        # Each scenario's bad debt is reduced by the total protection available,
        # floored at 0 (protection cannot create a profit).
        if protection_usd > 0:
            net_bad_debt_total = np.maximum(0.0, gross_bad_debt_total - protection_usd)
        else:
            net_bad_debt_total = gross_bad_debt_total

        scen_names = np.arange(N_SCEN)  # or whatever label you want
        summary = pd.DataFrame(
            {
                "scenario": scen_names,
                "bad_debt_ead_total": ead_totals,
                "recoveries_total": recoveries_totals,
                "gross_bad_debt_total": gross_bad_debt_total,
                "net_bad_debt_total": net_bad_debt_total,
                "debt_repaid_total": debt_repaid_totals,
                "collateral_liquidated_total": collat_liq_totals,
                "final_total_debt": final_debt_totals,
                "final_total_collateral": final_collat_totals,
                "max_delta_ltv": max_delta_ltv,
                "max_pct_loss": max_pct_loss,
                "prob_of_liq": pct_user_liq,
                "prob_of_default": pct_user_default,
            }
        )

        summary_df = (
            summary[
                [
                    "scenario",
                    "max_delta_ltv",
                    "net_bad_debt_total",
                    "gross_bad_debt_total",
                    "max_pct_loss",
                    "prob_of_liq",
                    "prob_of_default",
                ]
            ]
            .copy()
            .reset_index(drop=True)
        )

        bad_debt_var = np.quantile(summary_df["net_bad_debt_total"], perc, method="inverted_cdf")
        bad_debt_var_gross = np.quantile(summary_df["gross_bad_debt_total"], perc, method="inverted_cdf")  # noqa: F841

        # ES: always average the top ceil((1-perc)*N) scenarios by rank, regardless of ties.
        # Using >= bad_debt_var would include ALL zero-loss rows when VaR=0, inflating the tail set.
        n_tail = max(1, int(np.ceil((1 - perc) * len(summary_df))))
        bad_debt_es = summary_df["net_bad_debt_total"].nlargest(n_tail).mean()
        bad_debt_es_gross = summary_df["gross_bad_debt_total"].nlargest(n_tail).mean()
        crr = bad_debt_var / TOT_DEBT
        es = bad_debt_es / TOT_DEBT
        es_gross = bad_debt_es_gross / TOT_DEBT  # noqa: F841

        bins = [0.0, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 1.0]
        labels = ["<50%", "50–60%", "60–70%", "70–75%", "75–80%", "80–85%", ">85%"]

        # Create bucket once
        users_final["ltv_bucket"] = pd.cut(users_final["ltv"], bins=bins, labels=labels, right=False)

        ltv_bucket_stats = (
            users_final.groupby("ltv_bucket", observed=True)
            .agg(positions=("ltv", "size"), total_borrow_usd=("total_borrow_usd", "sum"), avg_ltv=("ltv", "mean"))
            .sort_index()
        )

        print("\n--------------------------------\n")
        print("Initial LTV bucket distribution:\n")
        for bucket, row in ltv_bucket_stats.iterrows():
            print(
                f"{row['positions']:>4} positions in {bucket:<7}\n"
                f"Borrowed: ${row['total_borrow_usd']:,.0f}\n"
                f"Borrow concentration: {row['total_borrow_usd'] / TOT_DEBT:.2%}\n"
                f"Avg LTV: {row['avg_ltv']:.2%}\n"
            )
        print("--------------------------------\n")

        # ── Headline CRR metrics ──────────────────────────────────────────────
        # EL  = mean(net_bad_debt) / TOT_DEBT  (Basel Expected Loss)
        # HHI = borrower concentration index
        # adj = (1 − HHI) × EL + HHI × ES     (concentration-adjusted CRR)
        crr_el_raw = float(summary_df["net_bad_debt_total"].mean()) / TOT_DEBT  # ratio 0-1

        _hhi = None
        if "total_borrow_usd" in self.user_df.columns:
            _borrow = self.user_df["total_borrow_usd"].fillna(0)
            _active = _borrow[_borrow > 0]
            if len(_active) > 0 and _active.sum() > 0:
                _shares = _active / _active.sum()
                _hhi = float((_shares**2).sum())

        _w = _hhi if _hhi is not None else 0.0
        # crr_adj_raw = crr_el_raw * (1 + _CRR_HHI_K * _w)

        # print(f"\nCRR as Net ES at {perc:.2%}: {es:.4%}")
        # print(f"CRR as Gross ES at {perc:.2%}: {es_gross:.4%}")
        # print(f"CRR as VaR at {perc:.2%}: {crr:.4%}")
        print(f"CRR as EL: {crr_el_raw:.4%}")
        print(f"HHI: {_hhi:.4%}" if _hhi is not None else "HHI: N/A")
        # print(f"PL at {perc:.2%}: {np.quantile(summary_df['prob_of_liq'], perc, method='inverted_cdf'):.4%}")
        # print(f"PD at {perc:.2%}: {np.quantile(summary_df['prob_of_default'], perc, method='inverted_cdf'):.4%}")
        print(f"Delta LTV at {perc:.2%}: {np.quantile(summary_df['max_delta_ltv'], perc, method='inverted_cdf'):.4%}\n")

        return {
            "summary_df": summary_df,
            "crr_el": round(crr_el_raw * 100, 6),  # % units
            "crr_es": round(es * 100, 6),  # % units
            "crr_var": round(crr * 100, 6),  # % units
            "hhi": round(_hhi * 100, 6) if _hhi is not None else None,
        }
