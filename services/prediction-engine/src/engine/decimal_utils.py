"""Shared decimal conversion and quantization helpers.

Used by both prediction and recommendation orchestrators to handle
JSON-origin values and standardize decimal precision.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from shared.config.errors import WeatherBotError

_CENT = Decimal("0.01")
_BASIS_POINT = Decimal("0.0001")


def decimal_from_json(value: Any, *, source: str) -> Decimal:
    """Safely convert a JSON-origin value to Decimal.

    Args:
        value: Raw value from JSON payload.
        source: Error source label for diagnostics.

    Raises:
        WeatherBotError: If conversion fails or type is unsupported.
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float | str):
        try:
            return Decimal(str(value))
        except InvalidOperation as exc:
            raise WeatherBotError(
                f"Invalid decimal payload: {value!r}",
                source=source,
            ) from exc
    raise WeatherBotError(
        f"Unsupported decimal payload type: {type(value).__name__}",
        source=source,
    )


def quantize_cents(value: Decimal) -> Decimal:
    """Quantize to 2 decimal places (cents)."""
    return value.quantize(_CENT)


def quantize_probability(value: Decimal) -> Decimal:
    """Quantize to 4 decimal places (basis points)."""
    return value.quantize(_BASIS_POINT)
