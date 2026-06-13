# CORE Model – General Guidelines for CRR

*BA Labs | 06/05/2026*

---

## 0. General Overview

The CORE model produces a Capital Requirement Ratio (CRR), which is the expected bad debt as a percentage of total loan exposure across thousands of simulated market scenarios. A higher CRR means the portfolio is expected to generate more unrecovered losses on average under stressed conditions.

---

## 1. Loan-to-Value: The Single Biggest Lever

The model is highly non-linear in LTV. The distance between a borrower's current LTV and the liquidation threshold (LT) is what matters: it determines how large a price drop is needed before a position becomes eligible for liquidation.

**Guidelines:**

- Target initial LTVs well below the liquidation threshold. Portfolios where most borrowers sit 'far away' from LTs (the amount depends on collaterals) produce near-zero CRR under normal volatility assumptions.
- Portfolios with LTVs tightly clustered are in a danger zone. Even a modest increase in volatility or a slightly longer stress horizon can cause CRR to increase sharply.
- The improvement is non-linear, so pushing from LTV 0.80 to 0.75 does more than pushing from 0.65 to 0.60.
- For more volatile collateral (see Section 3), apply tighter LTV ceilings. A 70% LTV on BTC may be equivalent in risk terms to a 55% LTV on a higher volatility token.

---

## 2. Collateral Quality: Volatility and Liquidity

The model treats volatility and order book liquidity as distinct risk drivers, and both increase CRR.

**On volatility:**

- The simulated price distribution widens with higher historical volatility, pushing more scenarios past the liquidation threshold.
- Newer tokens with short price histories receive a vol penalty in the model (the Lindy factor): a token with only 6 months of history can have its effective volatility doubled. This translates directly into higher CRR and should be reflected in more conservative LTV limits.
- Prefer collateral with at least 2–3 years of price history. For tokens with less than 1 year of history, apply significant LTV haircuts and limit position size.

**On liquidity:**

- The model simulates actual liquidation execution using real order book depth. If a liquidation is unprofitable (because slippage exceeds the liquidation bonus), that amount becomes bad debt.
- Every token is supported by aggregated order books across 11+ venues.
- Smaller or less traded tokens have substantially thinner depth. Even if such a token represents a minority of the portfolio by value, it can dominate overall CRR because it is precisely in those positions that liquidators are least likely to execute profitably under stress.
- Avoid accepting illiquid tokens as primary collateral for large loan sizes. If illiquid collateral is accepted, apply a material LTV reduction and treat this as a hard cap on position size.

---

## 3. Diversification: Collateral Type and Borrower Size

The model operates at individual position level and captures two distinct channels of concentration risk.

**Borrower concentration:**

- A portfolio of many small borrowers benefits from statistical diversification, and the model prices this indirectly through the liquidation engine: slippage is computed independently for each borrower position, so 100 small liquidations each consume a modest slice of order book depth and are individually more likely to be profitable.
- Monitor the HHI (Herfindahl-Hirschman Index). An HHI above 2500 — roughly equivalent to fewer than 4 equal-sized borrowers — signals that CRR is being driven by concentration, not by the average quality of the loan book.
- If the top 3 borrowers represent more than 50% of total exposure, concentration risk is a material driver of CRR: apply material reduction to LTVs.

**Collateral diversification:**

- When multiple collateral types are present, their correlation matters. The model uses a t-Copula, which reflects the empirical observation that crypto assets are more likely to crash together in extreme scenarios than standard correlation assumptions would imply.
- Adding a new collateral type that is highly correlated with existing ones does not reduce CRR meaningfully — it introduces the same tail risk through a different route.
- True diversification means accepting collateral tokens whose prices are driven by different fundamental factors. Liquid Staking Tokens (e.g., wstETH) backed by the same underlying as ETH add less diversification than their separate ticker might suggest.

---

## 4. Structural Parameters

Some variables are set at the protocol level (e.g. liquidation bonus) and understanding their direction helps interpret CRR numbers.

- A higher liquidation bonus incentivises liquidators to act, reducing the probability of failed liquidations. However, a higher bonus also means more collateral is seized per event, which increases sale size and slippage. The net effect depends on order book depth.
- If the protocol has a protection layer (e.g., a first-loss tranche or an insurance fund), the model separates gross CRR (before protection) from net CRR (after protection). Net CRR may be near zero even when gross CRR is material. Always request both figures: the gross number reflects structural fragility of the book.

---

## 5. Quick Reference: What Raises and Lowers CRR

| Factor | Direction | Actions |
|---|---|---|
| Initial LTV closer to LT | CRR ↑ | Impose LTV ceilings with meaningful buffer below LT |
| More volatile collateral | CRR ↑ | Apply tighter LTVs; weight toward BTC/ETH |
| Shorter collateral price history | CRR ↑ | LTV haircut + position size cap |
| Fewer, larger borrowers | CRR ↑ | Monitor HHI; enforce per-borrower concentration limits |
| Illiquid collateral token | CRR ↑ | Hard cap on loan size; significant LTV reduction |
| Higher cross-asset correlation | CRR ↑ | Seek genuinely diversified collateral types |
| Deeper order book / more liquid collateral | CRR ↓ | Prefer BTC, ETH, and major LSTs as primary collateral |
| Active protection layer (FLC, insurance) | Net CRR ↓ | Add these informations if any |

---

## 6. Examples: CRR Calculation Sensitivity

This section illustrates how the CRR responds to changes in leverage and borrower concentration.

**Baseline scenario:**

- Single borrowing position (HHI = 100%)
- BTC as collateral
- Borrow value: $150M in USDC
- LTV: 80%
- Liquidation Threshold (LT): 90%
- Liquidation bonus: 4%

**Sensitivity adjustments:**

- **A** — Reducing LTV by 10 percentage points (from 80% to 70%)
- **B** — Reducing concentration risk by splitting the exposure across multiple borrowers
- **C** — Both A and B together
- **D** — Substituting BTC with XRP
- **E** — Sensitivities A, B and D together

| Case | Scenario | CRR (%) | HHI (%) |
|---|---|---|---|
| – | Baseline (LTV = 80%, 1 pos, BTC) | 18.25 | 100 |
| A | Lower LTV (70%, 1 pos, BTC) | 5.84 | 100 |
| B | Diversified (80%, 10 pos, BTC) | 6.20 | 10 |
| C | Both A and B (70%, 10 pos, BTC) | 1.59 | 10 |
| D | BTC → XRP (80%, 1 pos, XRP) | 26.15 | 100 |
| E | A & B & D (70%, 10 pos, XRP) | 8.12 | 10 |

**Conclusions:**

The analysis shows that CRR is driven by LTVs, concentration, and collateral risk, with unequal impact across factors.

Reducing LTV is the most effective lever: lowering it from 80% to 70% (Case A) materially decreases CRR by increasing collateral buffers. Diversification (Case B) also reduces risk, but to a lesser extent, mainly addressing idiosyncratic borrower risk rather than systemic exposure. When combined (Case C), these measures produce strong compounding benefits under BTC collateral.

However, collateral volatility remains the dominant constraint. Switching to a more volatile asset (Case D) significantly increases CRR, even with unchanged LTV and concentration. Even under optimised conditions (Case E), CRR stays materially higher than in the BTC baseline.

Importantly, this effect is not driven by volatility alone. More volatile assets are typically associated with thinner order book liquidity and higher market impact, which amplifies liquidation risk. As a result, CRR reflects a joint effect of price volatility and market liquidity, both of which deteriorate the efficiency of collateral liquidation under stress.

This implies that risk parameters must be collateral-dependent. More volatile assets require lower LTVs and stricter concentration limits. In short, LTV and diversification are conditional risk controls: their effectiveness depends on the underlying collateral's risk profile, which ultimately sets the floor for achievable risk reduction.
