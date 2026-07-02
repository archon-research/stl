"""Reference: A.3.2.2.1.1.1.1.1.2 — Calculate Loss Given Default"""


def loss_given_default(
    liquidation_penalty: float,
    slippage: float,
    liquidation_threshold: float,
) -> float:
    """
    Calculate the loss given default for a collateral position.

    LGD = min(1, max(0, 1 - (1 - LP)(1 - S) / LT))

    Parameters:
        liquidation_penalty: LP, contractually agreed liquidation penalty [0, 1]
        slippage: S, estimated slippage for liquidating the position [0, 1]
        liquidation_threshold: LT, debt as fraction of collateral at liquidation [0, 1]

    Returns a value in [0, 1]. Zero means no loss; 1 means total loss.

    :source_uuid: c9bd4928-d054-4e89-9a98-720c439b0db3
    """
    recovery: float = (1.0 - liquidation_penalty) * (1.0 - slippage) / liquidation_threshold
    return max(0.0, min(1.0, 1.0 - recovery))
