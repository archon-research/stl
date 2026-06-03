import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Change each user LTV under the worst case scenario assumption HF = 1
# ──────────────────────────────────────────────────────────────────────────────


def change_user_ltvs(users_df: pd.DataFrame, market_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adjusts user collateral positions so that every user's health factor equals 1.

    Recomputes the total collateral USD required to exactly back each user's
    borrow at their liquidation LTV, then redistributes that collateral across
    individual supply columns proportionally to their original weights.
    Both USD and quantity columns are updated accordingly using oracle prices.

    Args:
        users_df:   DataFrame of user positions, including supply/borrow USD
                    columns, quantity supply columns, and an 'lltv' column.
        market_df:  DataFrame of market data with 'token_symbol' and
                    'oracle_price' columns used for USD-to-quantity conversion.

    Returns:
        Copy of users_df with updated individual supply USD and quantity
        columns, and health factor set to 1.0 for all users.
    """
    new_user_df = users_df.fillna(0).copy()
    oracle_price_dict = market_df.set_index("token_symbol")["oracle_price"].to_dict()

    new_user_df["new_total_collateral_usd"] = new_user_df["total_borrow_usd"].div(new_user_df["lltv"].replace(0, pd.NA))

    new_user_df["new_health_factor"] = 1.0

    # Select asset supply columns
    columns_usd = [col for col in new_user_df.columns if "_supply_usd" in col and "total_supply_usd" not in col]
    columns_qty = [
        col
        for col in new_user_df.columns
        if "_supply" in col and "_supply_usd" not in col and "total_supply" not in col
    ]

    new_supply_usd_df = new_user_df[columns_usd].copy()

    row_sums = new_supply_usd_df.sum(axis=1)

    weights = new_supply_usd_df.div(row_sums.replace(0, pd.NA), axis=0).fillna(0)

    new_supply_usd_df = weights.mul(new_user_df["new_total_collateral_usd"], axis=0)

    new_supply_qty_df = pd.DataFrame(index=new_supply_usd_df.index)

    for usd_col in columns_usd:
        token = usd_col.replace("_supply_usd", "").upper()
        qty_col = usd_col.replace("_supply_usd", "_supply")

        price = oracle_price_dict.get(token, 0)

        if price == 0:
            new_supply_qty_df[qty_col] = 0
        else:
            new_supply_qty_df[qty_col] = new_supply_usd_df[usd_col] / price

    base_df = new_user_df.drop(columns=columns_usd + columns_qty + ["new_total_collateral_usd"])

    final_df = pd.concat([base_df, new_supply_usd_df, new_supply_qty_df], axis=1)

    return final_df
