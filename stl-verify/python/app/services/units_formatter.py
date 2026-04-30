"""Display formatting utilities for consistent units and labels across the API."""

from decimal import Decimal


class UnitsFormatter:
    """Standardized display formatters for values across allocations/risk/prices."""

    @staticmethod
    def format_usd_amount(value: Decimal | float, precision: int = 2) -> str:
        """Format USD amount with $ prefix and thousands separator.

        Examples:
            1234567.89 -> "$1,234,567.89"
            0.001 -> "$0.00" (if precision=2)
        """
        if isinstance(value, float):
            value = Decimal(str(value))
        if not isinstance(value, Decimal):
            value = Decimal(value)

        formatted = "{:,.2f}".format(value)
        return f"${formatted}"

    @staticmethod
    def format_percentage(value: Decimal | float, precision: int = 2) -> str:
        """Format percentage with % suffix.

        Examples:
            0.1234 -> "12.34%"
            0.05 -> "5.00%"
        """
        if isinstance(value, float):
            value = Decimal(str(value))
        if not isinstance(value, Decimal):
            value = Decimal(value)

        pct_value = value * 100
        formatted = "{:.{precision}f}".format(pct_value, precision=precision)
        return f"{formatted}%"

    @staticmethod
    def format_token_amount(value: Decimal | float, symbol: str = "", decimals: int = 18) -> str:
        """Format token amount with optional symbol suffix.

        Examples:
            format_token_amount(1.5, "ETH") -> "1.50 ETH"
            format_token_amount(123456789, decimals=6) -> "123.46" (assuming 6 decimals)
        """
        if isinstance(value, float):
            value = Decimal(str(value))
        if not isinstance(value, Decimal):
            value = Decimal(value)

        # Scale by decimals if raw value (for onchain amounts)
        scaled = value / (10**decimals) if value > 10 else value
        formatted = "{:,.4f}".format(scaled).rstrip("0").rstrip(".")

        return f"{formatted} {symbol}".strip() if symbol else formatted

    @staticmethod
    def format_price(value: Decimal | float, precision: int = 4) -> str:
        """Format token price (USD per token) with $ prefix.

        Examples:
            1234.567 -> "$1,234.5670"
            0.0001 -> "$0.0001"
        """
        if isinstance(value, float):
            value = Decimal(str(value))
        if not isinstance(value, Decimal):
            value = Decimal(value)

        formatted = "{:,.{precision}f}".format(value, precision=precision)
        return f"${formatted}"

    @staticmethod
    def format_ratio(value: Decimal | float, precision: int = 4) -> str:
        """Format numeric ratio (unitless, e.g., 1.5 = 150% collateralization).

        Examples:
            1.5 -> "1.5000x"
            0.85 -> "0.8500x"
        """
        if isinstance(value, float):
            value = Decimal(str(value))
        if not isinstance(value, Decimal):
            value = Decimal(value)

        formatted = "{:.{precision}f}".format(value, precision=precision)
        return f"{formatted}x"

    @staticmethod
    def format_delta(current: Decimal | float, prior: Decimal | float, unit: str = "%") -> str:
        """Format 24h/relative delta with +/- prefix.

        Args:
            current: current value
            prior: prior value (for absolute delta calculation)
            unit: suffix ("%" for percentage deltas, "$" for USD, "x" for ratios)

        Examples:
            format_delta(100, 90, "%") -> "+11.11%"
            format_delta(50, 60, "$") -> "-$10.00"
        """
        if isinstance(current, float):
            current = Decimal(str(current))
        if isinstance(prior, float):
            prior = Decimal(str(prior))

        if prior == 0:
            return "N/A"

        delta = current - prior
        if unit == "%":
            pct_delta = (delta / prior) * 100
            sign = "+" if pct_delta >= 0 else ""
            return f"{sign}{pct_delta:.2f}%"
        elif unit == "$":
            sign = "+" if delta >= 0 else ""
            formatted = "{:,.2f}".format(delta)
            return f"{sign}${formatted}"
        elif unit == "x":
            ratio_delta = delta
            sign = "+" if ratio_delta >= 0 else ""
            formatted = "{:.4f}".format(ratio_delta)
            return f"{sign}{formatted}x"
        else:
            # Generic numeric delta
            sign = "+" if delta >= 0 else ""
            return f"{sign}{delta}"

    @staticmethod
    def get_freshness_label(timestamp_seconds: int, now_seconds: int) -> str:
        """Convert timestamp to relative freshness label.

        Examples:
            "just now" (< 1 min)
            "5m ago"
            "1h ago"
            "2d ago"
        """
        delta_seconds = now_seconds - timestamp_seconds
        if delta_seconds < 0:
            return "in the future"

        if delta_seconds < 60:
            return "just now"
        elif delta_seconds < 3600:
            minutes = delta_seconds // 60
            return f"{minutes}m ago"
        elif delta_seconds < 86400:
            hours = delta_seconds // 3600
            return f"{hours}h ago"
        elif delta_seconds < 604800:
            days = delta_seconds // 86400
            return f"{days}d ago"
        else:
            weeks = delta_seconds // 604800
            return f"{weeks}w ago"
