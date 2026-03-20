"""
Kalshi market data client for weather temperature contracts.

REST-only MVP. Uses pykalshi directly. Single class with three method groups:
1. discover_markets() — find weather bracket markets for cities and dates
2. fetch_snapshots() — get current prices for tracked market tickers
3. check_settlements() — find newly settled markets

Architecture decisions (Sprint 1 review):
- REST-only: weather markets settle daily, 2-5 min polling is sufficient
- pykalshi direct: no thin wrapper, use pykalshi types where convenient
- Hardcoded series patterns (KXHIGH{code}, KXLOW{code}) + dynamic bracket discovery
"""

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from pykalshi import (
    KalshiClient as PyKalshiClient,
    Market as PyKalshiMarket,
    MarketStatus as KalshiMarketStatus,
)
from pykalshi.exceptions import (
    AuthenticationError,
    KalshiAPIError,
    RateLimitError,
    ResourceNotFoundError,
)

from shared.config.cities import CITIES
from shared.config.errors import KalshiApiError
from shared.config.logging import get_logger
from shared.db.enums import MarketStatus, MarketType

logger = get_logger("kalshi-client")

# ---------------------------------------------------------------------------
# Series ticker patterns
# ---------------------------------------------------------------------------

SERIES_HIGH_PREFIX = "KXHIGH"
SERIES_LOW_PREFIX = "KXLOW"

# ---------------------------------------------------------------------------
# Data models returned by KalshiClient methods
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DiscoveredMarket:
    """A weather bracket market discovered from the Kalshi API."""

    event_ticker: str
    market_ticker: str
    city_code: str
    forecast_date: date
    market_type: MarketType
    bracket_low: Decimal | None
    bracket_high: Decimal | None
    is_edge_bracket: bool
    yes_bid: Decimal | None
    yes_ask: Decimal | None
    no_bid: Decimal | None
    no_ask: Decimal | None
    volume: int | None
    open_interest: int | None
    status: MarketStatus


@dataclass(frozen=True)
class MarketSnapshot:
    """A price snapshot for a single market at a point in time."""

    ticker: str
    timestamp: datetime
    yes_bid: Decimal | None
    yes_ask: Decimal | None
    no_bid: Decimal | None
    no_ask: Decimal | None
    volume: int | None
    open_interest: int | None
    last_price: Decimal | None


@dataclass(frozen=True)
class SettledMarket:
    """Settlement info for a resolved or closed market."""

    ticker: str
    result: str  # "yes", "no", or "" for closed without result
    settlement_value: Decimal | None
    final_status: MarketStatus  # SETTLED or CLOSED


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _parse_numeric_decimal(
    value: str | float | int | Decimal | None,
) -> Decimal | None:
    """Parse a finite numeric value into Decimal, or None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None

    raw_value: str | int | Decimal
    if isinstance(value, str):
        raw_value = value.strip()
        if raw_value == "":
            return None
    elif isinstance(value, float):
        raw_value = str(value)
    else:
        raw_value = value

    try:
        parsed = Decimal(raw_value)
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _to_decimal(value: str | float | int | Decimal | None) -> Decimal | None:
    """Convert a finite numeric input to Decimal, or None."""
    return _parse_numeric_decimal(value)


def _to_int(value: str | float | int | Decimal | None) -> int | None:
    """Convert an integral finite numeric input to int, or None."""
    parsed = _parse_numeric_decimal(value)
    if parsed is None:
        return None
    if parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def map_kalshi_status(
    kalshi_status: KalshiMarketStatus | None,
    *,
    ticker: str | None = None,
    correlation_id: str | None = None,
) -> MarketStatus:
    """Map pykalshi MarketStatus to our MarketStatus enum."""
    raw_status = None if kalshi_status is None else getattr(kalshi_status, "value", None)
    if not isinstance(raw_status, str):
        logger.error(
            "kalshi_unknown_market_status",
            ticker=ticker,
            raw_status=raw_status,
            correlation_id=correlation_id,
            alert="kalshi_unknown_market_status",
        )
        raise KalshiApiError(
            "Kalshi market status was missing",
            correlation_id=correlation_id,
        )

    val = raw_status.lower()
    if val in ("settled", "finalized", "determined"):
        return MarketStatus.SETTLED
    if val in ("closed", "inactive"):
        return MarketStatus.CLOSED
    if val in ("open", "active"):
        return MarketStatus.ACTIVE

    logger.error(
        "kalshi_unknown_market_status",
        ticker=ticker,
        raw_status=raw_status,
        correlation_id=correlation_id,
        alert="kalshi_unknown_market_status",
    )
    raise KalshiApiError(
        f"Unknown Kalshi market status: {raw_status!r}",
        correlation_id=correlation_id,
    )


# ---------------------------------------------------------------------------
# Bracket parsing — extract temperature range from market subtitle/title
# ---------------------------------------------------------------------------

# "62°F to 63°F", "62 to 63", "62°F - 63°F"
_RANGE_PATTERN = re.compile(
    r"(-?\d+(?:\.\d+)?)\s*°?\s*F?\s*(?:to|-)\s*(-?\d+(?:\.\d+)?)\s*°?\s*F?"
)
# "Below 50°F", "under 50", "49°F or less", "49 or lower", "49 or below"
_BELOW_PATTERN = re.compile(
    r"(?:below|under|less than)\s*(-?\d+(?:\.\d+)?)\s*°?\s*F?",
    re.IGNORECASE,
)
_OR_LESS_PATTERN = re.compile(
    r"(-?\d+(?:\.\d+)?)\s*°?\s*F?\s*or\s*(?:less|lower|below)",
    re.IGNORECASE,
)
# "72°F or above", "72 or more", "72+", "above 72"
_OR_ABOVE_PATTERN = re.compile(
    r"(-?\d+(?:\.\d+)?)\s*°?\s*F?\s*(?:or\s*(?:more|above|higher|greater)|\+)",
    re.IGNORECASE,
)
_ABOVE_PREFIX_PATTERN = re.compile(
    r"(?:above|over|more than|at least|greater than)\s*(-?\d+(?:\.\d+)?)\s*°?\s*F?",
    re.IGNORECASE,
)


def parse_bracket(subtitle: str | None) -> tuple[Decimal | None, Decimal | None, bool]:
    """Parse bracket boundaries from a market subtitle.

    Returns:
        (bracket_low, bracket_high, is_edge_bracket)

    Examples:
        "62°F to 63°F" → (Decimal("62"), Decimal("63"), False)
        "Below 50°F"   → (None, Decimal("50"), True)
        "72°F or above" → (Decimal("72"), None, True)
        None            → (None, None, False)
    """
    if not subtitle:
        return None, None, False

    # Range bracket (most common)
    match = _RANGE_PATTERN.search(subtitle)
    if match:
        return Decimal(match.group(1)), Decimal(match.group(2)), False

    # "Below X" / "under X" / "less than X"
    match = _BELOW_PATTERN.search(subtitle)
    if match:
        return None, Decimal(match.group(1)), True

    # "X or less" / "X or lower" / "X or below"
    match = _OR_LESS_PATTERN.search(subtitle)
    if match:
        return None, Decimal(match.group(1)), True

    # "X or above" / "X or more" / "X+"
    match = _OR_ABOVE_PATTERN.search(subtitle)
    if match:
        return Decimal(match.group(1)), None, True

    # "above X" / "over X" / "more than X"
    match = _ABOVE_PREFIX_PATTERN.search(subtitle)
    if match:
        return Decimal(match.group(1)), None, True

    return None, None, False


# ---------------------------------------------------------------------------
# Ticker/date parsing
# ---------------------------------------------------------------------------

_MONTH_MAP: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Matches date segment in tickers like "KXHIGHNYC-26MAR16-T62"
_TICKER_DATE_PATTERN = re.compile(r"-(\d{2})([A-Z]{3})(\d{2})-")


def parse_date_from_ticker(ticker: str) -> date | None:
    """Extract the forecast date from a Kalshi market ticker.

    Ticker format: KXHIGH{CITY}-{YY}{MON}{DD}-T{temp}
    Example: KXHIGHNYC-26MAR16-T62 → date(2026, 3, 16)
    """
    match = _TICKER_DATE_PATTERN.search(ticker)
    if not match:
        return None
    year = 2000 + int(match.group(1))
    month = _MONTH_MAP.get(match.group(2))
    day = int(match.group(3))
    if month is None:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def infer_market_type(series_ticker: str) -> MarketType:
    """Determine HIGH or LOW from the series ticker prefix."""
    upper = series_ticker.upper()
    if upper.startswith(SERIES_HIGH_PREFIX):
        return MarketType.HIGH
    if upper.startswith(SERIES_LOW_PREFIX):
        return MarketType.LOW
    # Fallback: check if "LOW" appears anywhere in the ticker
    if "LOW" in upper:
        return MarketType.LOW
    return MarketType.HIGH


def extract_city_code(series_ticker: str) -> str | None:
    """Extract city code from a series ticker like 'KXHIGHNYC' → 'NYC'."""
    upper = series_ticker.upper()
    if upper.startswith(SERIES_HIGH_PREFIX):
        return upper[len(SERIES_HIGH_PREFIX):]
    if upper.startswith(SERIES_LOW_PREFIX):
        return upper[len(SERIES_LOW_PREFIX):]
    return None


def _fetch_markets(client: PyKalshiClient, **kwargs: Any) -> list[PyKalshiMarket]:
    """Call pykalshi get_markets() with a typed return.

    pykalshi's get_markets() uses ``**extra_params: Unknown`` which makes the
    return type partially unknown to pyright in strict mode.  This helper
    casts the result to a concrete list so callers get proper type checking.
    """
    result = client.get_markets(**kwargs)  # type: ignore[reportUnknownMemberType]
    return cast(list[PyKalshiMarket], list(result))


def _wrap_market_fetch_error(
    *,
    operation: str,
    exc: Exception,
    correlation_id: str | None,
) -> KalshiApiError:
    return KalshiApiError(
        f"Failed to {operation}: {exc}",
        correlation_id=correlation_id,
    )


# ---------------------------------------------------------------------------
# KalshiClient
# ---------------------------------------------------------------------------

# Maximum tickers per batch in get_markets (Kalshi API limit)
_BATCH_SIZE = 100


class KalshiClient:
    """Kalshi market data client for weather temperature contracts.

    Uses pykalshi.KalshiClient directly for API communication.

    Args:
        client: A pre-configured pykalshi.KalshiClient instance.
    """

    def __init__(self, client: PyKalshiClient) -> None:
        self._client = client
        self._last_discovery_had_errors = False

    @property
    def last_discovery_had_errors(self) -> bool:
        """Whether the most recent discover_markets() call had series fetch errors."""
        return self._last_discovery_had_errors

    @classmethod
    def from_settings(
        cls,
        *,
        api_key_id: str,
        private_key_path: str,
    ) -> "KalshiClient":
        """Create a KalshiClient from API credentials.

        Raises:
            KalshiApiError: If pykalshi client initialization fails.
        """
        try:
            pykalshi = PyKalshiClient(
                api_key_id=api_key_id,
                private_key_path=private_key_path,
            )
        except Exception as exc:
            raise KalshiApiError(
                f"Failed to initialize Kalshi client: {exc}",
            ) from exc
        return cls(pykalshi)

    def close(self) -> None:
        """Close the underlying pykalshi client."""
        self._client.close()

    def __enter__(self) -> "KalshiClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Helper: build series tickers
    # ------------------------------------------------------------------

    @staticmethod
    def build_series_tickers(
        city_codes: list[str] | None = None,
    ) -> list[tuple[str, str, MarketType]]:
        """Build (series_ticker, city_code, market_type) tuples.

        Args:
            city_codes: City ticker codes.  Defaults to all configured cities.

        Returns:
            List of (series_ticker, city_code, MarketType) tuples.
        """
        codes = city_codes or list(CITIES.keys())
        result: list[tuple[str, str, MarketType]] = []
        for code in codes:
            result.append((f"{SERIES_HIGH_PREFIX}{code}", code, MarketType.HIGH))
            result.append((f"{SERIES_LOW_PREFIX}{code}", code, MarketType.LOW))
        return result

    # ------------------------------------------------------------------
    # 1. Market Discovery
    # ------------------------------------------------------------------

    def discover_markets(
        self,
        city_codes: list[str] | None = None,
        forecast_date: date | None = None,
        *,
        correlation_id: str | None = None,
    ) -> list[DiscoveredMarket]:
        """Discover weather bracket markets from Kalshi.

        Queries each series ticker (KXHIGH{code}, KXLOW{code}) and returns
        all bracket markets, optionally filtered to a specific date.

        Args:
            city_codes: City ticker codes to query.  Defaults to all cities.
            forecast_date: If given, only return markets for this date.
            correlation_id: For log tracing.

        Returns:
            List of DiscoveredMarket with bracket and price data.
        """
        series_list = self.build_series_tickers(city_codes)
        discovered: list[DiscoveredMarket] = []
        had_series_errors = False

        for series_ticker, city_code, market_type in series_list:
            try:
                markets = _fetch_markets(
                    self._client,
                    series_ticker=series_ticker,
                    fetch_all=True,
                )
            except ResourceNotFoundError:
                logger.debug(
                    "kalshi_series_not_found",
                    series_ticker=series_ticker,
                    city=city_code,
                    correlation_id=correlation_id,
                )
                continue
            except (AuthenticationError, RateLimitError, KalshiAPIError) as exc:
                had_series_errors = True
                logger.warning(
                    "kalshi_markets_fetch_failed",
                    series_ticker=series_ticker,
                    city=city_code,
                    error_type=type(exc).__name__,
                    correlation_id=correlation_id,
                )
                continue

            for market in markets:
                # Extract forecast date from ticker
                mkt_date = parse_date_from_ticker(market.ticker)
                if mkt_date is None:
                    logger.debug(
                        "kalshi_market_no_date",
                        ticker=market.ticker,
                        city=city_code,
                        correlation_id=correlation_id,
                    )
                    continue

                if forecast_date is not None and mkt_date != forecast_date:
                    continue

                try:
                    status = map_kalshi_status(
                        market.status,
                        ticker=market.ticker,
                        correlation_id=correlation_id,
                    )
                except KalshiApiError:
                    continue

                bracket_low, bracket_high, is_edge = parse_bracket(market.subtitle)

                discovered.append(DiscoveredMarket(
                    event_ticker=market.event_ticker or "",
                    market_ticker=market.ticker,
                    city_code=city_code,
                    forecast_date=mkt_date,
                    market_type=market_type,
                    bracket_low=bracket_low,
                    bracket_high=bracket_high,
                    is_edge_bracket=is_edge,
                    yes_bid=_to_decimal(market.yes_bid_dollars),
                    yes_ask=_to_decimal(market.yes_ask_dollars),
                    no_bid=_to_decimal(market.no_bid_dollars),
                    no_ask=_to_decimal(market.no_ask_dollars),
                    volume=_to_int(market.volume_fp),
                    open_interest=_to_int(market.open_interest_fp),
                    status=status,
                ))

        logger.info(
            "kalshi_discovery_complete",
            cities_queried=len(city_codes or list(CITIES.keys())),
            markets_found=len(discovered),
            forecast_date=str(forecast_date) if forecast_date else "all",
            had_series_errors=had_series_errors,
            correlation_id=correlation_id,
        )
        self._last_discovery_had_errors = had_series_errors

        return discovered

    # ------------------------------------------------------------------
    # 2. Price Snapshots
    # ------------------------------------------------------------------

    def fetch_snapshots(
        self,
        tickers: list[str],
        *,
        correlation_id: str | None = None,
    ) -> list[MarketSnapshot]:
        """Fetch current price snapshots for a list of market tickers.

        Args:
            tickers: Kalshi market tickers to fetch.
            correlation_id: For log tracing.

        Returns:
            List of MarketSnapshot with current price data.

        Raises:
            KalshiApiError: On API communication failures.
        """
        now = datetime.now(timezone.utc)
        snapshots: list[MarketSnapshot] = []

        for i in range(0, len(tickers), _BATCH_SIZE):
            batch = tickers[i : i + _BATCH_SIZE]
            try:
                markets = _fetch_markets(self._client, tickers=batch)
            except ResourceNotFoundError:
                # Batch contains an unknown ticker — retry individually
                # so one bad ticker doesn't block the entire batch.
                markets = []
                for ticker in batch:
                    try:
                        markets.extend(
                            _fetch_markets(self._client, tickers=[ticker])
                        )
                    except ResourceNotFoundError:
                        logger.warning(
                            "kalshi_snapshot_ticker_not_found",
                            ticker=ticker,
                            correlation_id=correlation_id,
                        )
                    except (AuthenticationError, RateLimitError, KalshiAPIError) as exc:
                        raise _wrap_market_fetch_error(
                            operation="fetch market snapshots",
                            exc=exc,
                            correlation_id=correlation_id,
                        ) from exc
            except (AuthenticationError, RateLimitError, KalshiAPIError) as exc:
                raise _wrap_market_fetch_error(
                    operation="fetch market snapshots",
                    exc=exc,
                    correlation_id=correlation_id,
                ) from exc

            for market in markets:
                snapshots.append(MarketSnapshot(
                    ticker=market.ticker,
                    timestamp=now,
                    yes_bid=_to_decimal(market.yes_bid_dollars),
                    yes_ask=_to_decimal(market.yes_ask_dollars),
                    no_bid=_to_decimal(market.no_bid_dollars),
                    no_ask=_to_decimal(market.no_ask_dollars),
                    volume=_to_int(market.volume_fp),
                    open_interest=_to_int(market.open_interest_fp),
                    last_price=_to_decimal(market.last_price_dollars),
                ))

        logger.info(
            "kalshi_snapshots_fetched",
            tickers_requested=len(tickers),
            snapshots_returned=len(snapshots),
            correlation_id=correlation_id,
        )

        return snapshots

    # ------------------------------------------------------------------
    # 3. Settlement Tracking
    # ------------------------------------------------------------------

    def check_settlements(
        self,
        tickers: list[str],
        *,
        correlation_id: str | None = None,
    ) -> list[SettledMarket]:
        """Check which of the given tickers have been settled or closed.

        Returns markets whose Kalshi status is SETTLED (with result) or
        CLOSED (without result). This prevents CLOSED markets from
        accumulating as zombie ACTIVE rows in the database.

        Args:
            tickers: Market tickers to check.
            correlation_id: For log tracing.

        Returns:
            List of SettledMarket for resolved or closed tickers.

        Raises:
            KalshiApiError: On API communication failures.
        """
        resolved: list[SettledMarket] = []

        for i in range(0, len(tickers), _BATCH_SIZE):
            batch = tickers[i : i + _BATCH_SIZE]
            try:
                markets = _fetch_markets(self._client, tickers=batch)
            except ResourceNotFoundError:
                # Batch contains an unknown ticker — retry individually
                # so one bad ticker doesn't block the entire batch.
                markets = []
                for ticker in batch:
                    try:
                        markets.extend(
                            _fetch_markets(self._client, tickers=[ticker])
                        )
                    except ResourceNotFoundError:
                        logger.warning(
                            "kalshi_settlement_ticker_not_found",
                            ticker=ticker,
                            correlation_id=correlation_id,
                        )
                    except (AuthenticationError, RateLimitError, KalshiAPIError) as exc:
                        raise _wrap_market_fetch_error(
                            operation="check settlements",
                            exc=exc,
                            correlation_id=correlation_id,
                        ) from exc
            except (AuthenticationError, RateLimitError, KalshiAPIError) as exc:
                raise _wrap_market_fetch_error(
                    operation="check settlements",
                    exc=exc,
                    correlation_id=correlation_id,
                ) from exc

            for market in markets:
                try:
                    status = map_kalshi_status(
                        market.status,
                        ticker=market.ticker,
                        correlation_id=correlation_id,
                    )
                except KalshiApiError:
                    continue
                if status in (MarketStatus.SETTLED, MarketStatus.CLOSED):
                    resolved.append(SettledMarket(
                        ticker=market.ticker,
                        result=market.result or "",
                        settlement_value=_to_decimal(
                            market.settlement_value_dollars
                        ),
                        final_status=status,
                    ))

        logger.info(
            "kalshi_settlements_checked",
            tickers_checked=len(tickers),
            settled_count=len(resolved),
            correlation_id=correlation_id,
        )

        return resolved
