"""Tests for KalshiClient: market discovery, price snapshots, settlement tracking.

Covers:
- Helper functions: _to_decimal, _to_int, map_kalshi_status
- Bracket parsing: range, edge (below/above), various formats, unparseable
- Ticker date parsing: valid tickers, invalid months, malformed tickers
- Series ticker construction: single city, all cities, HIGH/LOW pairing
- discover_markets: normal flow, date filtering, API errors, empty results
- fetch_snapshots: normal flow, batching, API errors
- check_settlements: settled vs active markets, API errors
- Context manager protocol
- from_settings factory
"""

from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pykalshi import MarketStatus as KalshiMarketStatus
from pykalshi.exceptions import (
    AuthenticationError,
    KalshiAPIError,
    RateLimitError,
    ResourceNotFoundError,
)

from shared.config.errors import KalshiApiError
from shared.db.enums import MarketStatus, MarketType

from src.clients.kalshi import (
    DiscoveredMarket,
    KalshiClient,
    MarketSnapshot,
    SettledMarket,
    extract_city_code,
    infer_market_type,
    _to_decimal,
    _to_int,
    map_kalshi_status,
    parse_bracket,
    parse_date_from_ticker,
)


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestToDecimal:
    def test_valid_string(self) -> None:
        assert _to_decimal("0.54") == Decimal("0.54")

    def test_zero(self) -> None:
        assert _to_decimal("0") == Decimal("0")

    def test_negative(self) -> None:
        assert _to_decimal("-1.23") == Decimal("-1.23")

    def test_none_returns_none(self) -> None:
        assert _to_decimal(None) is None

    def test_invalid_string_returns_none(self) -> None:
        assert _to_decimal("not-a-number") is None

    def test_empty_string_returns_none(self) -> None:
        assert _to_decimal("") is None


class TestToInt:
    def test_int_value(self) -> None:
        assert _to_int(42) == 42

    def test_float_value(self) -> None:
        assert _to_int(123.0) == 123

    def test_string_int(self) -> None:
        assert _to_int("99") == 99

    def test_string_float(self) -> None:
        assert _to_int("123.0") == 123

    def test_none_returns_none(self) -> None:
        assert _to_int(None) is None

    def test_invalid_string_returns_none(self) -> None:
        assert _to_int("abc") is None


class TestMapKalshiStatus:
    def test_settled(self) -> None:
        assert map_kalshi_status(KalshiMarketStatus.SETTLED) == MarketStatus.SETTLED

    def test_finalized(self) -> None:
        assert map_kalshi_status(KalshiMarketStatus.FINALIZED) == MarketStatus.SETTLED

    def test_determined(self) -> None:
        assert map_kalshi_status(KalshiMarketStatus.DETERMINED) == MarketStatus.SETTLED

    def test_closed(self) -> None:
        assert map_kalshi_status(KalshiMarketStatus.CLOSED) == MarketStatus.CLOSED

    def test_inactive(self) -> None:
        assert map_kalshi_status(KalshiMarketStatus.INACTIVE) == MarketStatus.CLOSED

    def test_open(self) -> None:
        assert map_kalshi_status(KalshiMarketStatus.OPEN) == MarketStatus.ACTIVE

    def test_active(self) -> None:
        assert map_kalshi_status(KalshiMarketStatus.ACTIVE) == MarketStatus.ACTIVE

    def test_none_defaults_active(self) -> None:
        assert map_kalshi_status(None) == MarketStatus.ACTIVE


# ---------------------------------------------------------------------------
# Bracket parsing tests
# ---------------------------------------------------------------------------


class TestParseBracket:
    """Test bracket extraction from market subtitles."""

    # --- Range brackets ---

    def test_range_degrees_f(self) -> None:
        assert parse_bracket("62°F to 63°F") == (Decimal("62"), Decimal("63"), False)

    def test_range_plain_numbers(self) -> None:
        assert parse_bracket("62 to 63") == (Decimal("62"), Decimal("63"), False)

    def test_range_dash_separator(self) -> None:
        assert parse_bracket("62°F - 63°F") == (Decimal("62"), Decimal("63"), False)

    def test_range_negative_temps(self) -> None:
        assert parse_bracket("-5°F to -3°F") == (Decimal("-5"), Decimal("-3"), False)

    def test_range_decimal_temps(self) -> None:
        assert parse_bracket("62.5 to 63.5") == (Decimal("62.5"), Decimal("63.5"), False)

    # --- Below/under edge brackets ---

    def test_below(self) -> None:
        assert parse_bracket("Below 50°F") == (None, Decimal("50"), True)

    def test_under(self) -> None:
        assert parse_bracket("Under 45") == (None, Decimal("45"), True)

    def test_less_than(self) -> None:
        assert parse_bracket("Less than 40°F") == (None, Decimal("40"), True)

    def test_or_less(self) -> None:
        assert parse_bracket("49°F or less") == (None, Decimal("49"), True)

    def test_or_lower(self) -> None:
        assert parse_bracket("49°F or lower") == (None, Decimal("49"), True)

    def test_or_below(self) -> None:
        assert parse_bracket("49 or below") == (None, Decimal("49"), True)

    # --- Above/over edge brackets ---

    def test_or_above(self) -> None:
        assert parse_bracket("72°F or above") == (Decimal("72"), None, True)

    def test_or_more(self) -> None:
        assert parse_bracket("72 or more") == (Decimal("72"), None, True)

    def test_or_higher(self) -> None:
        assert parse_bracket("72°F or higher") == (Decimal("72"), None, True)

    def test_plus_suffix(self) -> None:
        assert parse_bracket("72°F+") == (Decimal("72"), None, True)

    def test_above_prefix(self) -> None:
        assert parse_bracket("Above 72°F") == (Decimal("72"), None, True)

    def test_over_prefix(self) -> None:
        assert parse_bracket("Over 80") == (Decimal("80"), None, True)

    def test_more_than_prefix(self) -> None:
        assert parse_bracket("More than 90°F") == (Decimal("90"), None, True)

    # --- Edge cases ---

    def test_none_input(self) -> None:
        assert parse_bracket(None) == (None, None, False)

    def test_empty_string(self) -> None:
        assert parse_bracket("") == (None, None, False)

    def test_unparseable_returns_none(self) -> None:
        assert parse_bracket("Will it rain?") == (None, None, False)


# ---------------------------------------------------------------------------
# Ticker date parsing tests
# ---------------------------------------------------------------------------


class TestParseDateFromTicker:
    def test_standard_ticker(self) -> None:
        assert parse_date_from_ticker("KXHIGHNYC-26MAR16-T62") == date(2026, 3, 16)

    def test_low_ticker(self) -> None:
        assert parse_date_from_ticker("KXLOWCHI-26JAN05-T20") == date(2026, 1, 5)

    def test_december(self) -> None:
        assert parse_date_from_ticker("KXHIGHMIA-25DEC25-T80") == date(2025, 12, 25)

    def test_no_date_segment(self) -> None:
        assert parse_date_from_ticker("KXHIGHNYC") is None

    def test_invalid_month(self) -> None:
        assert parse_date_from_ticker("KXHIGHNYC-26XYZ16-T62") is None

    def test_invalid_day(self) -> None:
        # Feb 30 doesn't exist
        assert parse_date_from_ticker("KXHIGHNYC-26FEB30-T62") is None

    def test_empty_string(self) -> None:
        assert parse_date_from_ticker("") is None


class TestInferMarketType:
    def test_high(self) -> None:
        assert infer_market_type("KXHIGHNYC") == MarketType.HIGH

    def test_low(self) -> None:
        assert infer_market_type("KXLOWNYC") == MarketType.LOW

    def test_case_insensitive(self) -> None:
        assert infer_market_type("kxhighnyc") == MarketType.HIGH

    def test_unknown_defaults_high(self) -> None:
        assert infer_market_type("SOMETHING") == MarketType.HIGH

    def test_low_in_ticker(self) -> None:
        assert infer_market_type("SOMETHINGLOW") == MarketType.LOW


class TestExtractCityCode:
    def test_high_series(self) -> None:
        assert extract_city_code("KXHIGHNYC") == "NYC"

    def test_low_series(self) -> None:
        assert extract_city_code("KXLOWCHI") == "CHI"

    def test_unknown_prefix(self) -> None:
        assert extract_city_code("SOMETHING") is None


class TestBuildSeriesTickers:
    def test_single_city(self) -> None:
        result = KalshiClient.build_series_tickers(["NYC"])
        assert result == [
            ("KXHIGHNYC", "NYC", MarketType.HIGH),
            ("KXLOWNYC", "NYC", MarketType.LOW),
        ]

    def test_multiple_cities(self) -> None:
        result = KalshiClient.build_series_tickers(["NYC", "CHI"])
        assert len(result) == 4
        assert ("KXHIGHNYC", "NYC", MarketType.HIGH) in result
        assert ("KXLOWCHI", "CHI", MarketType.LOW) in result

    def test_defaults_to_all_cities(self) -> None:
        result = KalshiClient.build_series_tickers()
        # 42 cities × 2 (HIGH + LOW) = 84 (or however many cities we have)
        from shared.config.cities import CITIES
        assert len(result) == len(CITIES) * 2


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _make_mock_market(
    ticker: str = "KXHIGHNYC-26MAR16-T62",
    event_ticker: str = "KXHIGHNYC-26MAR16",
    subtitle: str = "62°F to 63°F",
    yes_bid_dollars: str | None = "0.50",
    yes_ask_dollars: str | None = "0.54",
    no_bid_dollars: str | None = "0.46",
    no_ask_dollars: str | None = "0.50",
    volume_fp: str | None = "150",
    open_interest_fp: str | None = "300",
    last_price_dollars: str | None = "0.52",
    status: KalshiMarketStatus | None = KalshiMarketStatus.OPEN,
    result: str | None = None,
    settlement_value_dollars: str | None = None,
) -> MagicMock:
    """Create a mock pykalshi Market object."""
    mock = MagicMock()
    mock.ticker = ticker
    mock.event_ticker = event_ticker
    mock.subtitle = subtitle
    mock.yes_bid_dollars = yes_bid_dollars
    mock.yes_ask_dollars = yes_ask_dollars
    mock.no_bid_dollars = no_bid_dollars
    mock.no_ask_dollars = no_ask_dollars
    mock.volume_fp = volume_fp
    mock.open_interest_fp = open_interest_fp
    mock.last_price_dollars = last_price_dollars
    mock.status = status
    mock.result = result
    mock.settlement_value_dollars = settlement_value_dollars
    return mock


def _make_kalshi_client() -> tuple[KalshiClient, MagicMock]:
    """Create a KalshiClient with a mocked pykalshi client."""
    mock_pykalshi = MagicMock()
    client = KalshiClient(mock_pykalshi)
    return client, mock_pykalshi


# ---------------------------------------------------------------------------
# KalshiClient.discover_markets tests
# ---------------------------------------------------------------------------


class TestDiscoverMarkets:
    def test_discovers_markets_for_single_city(self) -> None:
        client, mock = _make_kalshi_client()
        market = _make_mock_market()
        mock.get_markets.return_value = [market]

        result = client.discover_markets(
            city_codes=["NYC"], forecast_date=date(2026, 3, 16)
        )

        assert len(result) >= 1
        m = result[0]
        assert isinstance(m, DiscoveredMarket)
        assert m.market_ticker == "KXHIGHNYC-26MAR16-T62"
        assert m.city_code == "NYC"
        assert m.forecast_date == date(2026, 3, 16)
        assert m.market_type == MarketType.HIGH
        assert m.bracket_low == Decimal("62")
        assert m.bracket_high == Decimal("63")
        assert m.is_edge_bracket is False
        assert m.yes_bid == Decimal("0.50")
        assert m.yes_ask == Decimal("0.54")
        assert m.status == MarketStatus.ACTIVE

    def test_filters_by_date(self) -> None:
        client, mock = _make_kalshi_client()
        mar16 = _make_mock_market(ticker="KXHIGHNYC-26MAR16-T62")
        mar17 = _make_mock_market(ticker="KXHIGHNYC-26MAR17-T62")
        mock.get_markets.return_value = [mar16, mar17]

        result = client.discover_markets(
            city_codes=["NYC"], forecast_date=date(2026, 3, 16)
        )

        # Should only include the Mar 16 market (for HIGH series)
        tickers = [m.market_ticker for m in result if m.market_type == MarketType.HIGH]
        assert "KXHIGHNYC-26MAR16-T62" in tickers
        assert "KXHIGHNYC-26MAR17-T62" not in tickers

    def test_no_date_filter_returns_all(self) -> None:
        client, mock = _make_kalshi_client()
        mar16 = _make_mock_market(ticker="KXHIGHNYC-26MAR16-T62")
        mar17 = _make_mock_market(ticker="KXHIGHNYC-26MAR17-T62")
        mock.get_markets.return_value = [mar16, mar17]

        result = client.discover_markets(city_codes=["NYC"])

        # Both dates should appear (for HIGH at minimum)
        high_tickers = [m.market_ticker for m in result if m.market_type == MarketType.HIGH]
        assert "KXHIGHNYC-26MAR16-T62" in high_tickers
        assert "KXHIGHNYC-26MAR17-T62" in high_tickers

    def test_api_error_skips_series(self) -> None:
        """API error for one series should not prevent other series from being queried."""
        client, mock = _make_kalshi_client()

        def side_effect(*, series_ticker: str, fetch_all: bool) -> list[Any]:
            if "HIGH" in series_ticker:
                raise KalshiAPIError(500, "server error")
            return [_make_mock_market(
                ticker="KXLOWNYC-26MAR16-T50",
                event_ticker="KXLOWNYC-26MAR16",
                subtitle="50°F to 51°F",
            )]

        mock.get_markets.side_effect = side_effect

        result = client.discover_markets(
            city_codes=["NYC"], forecast_date=date(2026, 3, 16)
        )

        # LOW series should still succeed
        assert any(m.market_type == MarketType.LOW for m in result)

    def test_resource_not_found_skips_silently(self) -> None:
        client, mock = _make_kalshi_client()
        mock.get_markets.side_effect = ResourceNotFoundError(404, "not found")

        result = client.discover_markets(city_codes=["NYC"])

        assert result == []

    def test_empty_markets_returns_empty(self) -> None:
        client, mock = _make_kalshi_client()
        mock.get_markets.return_value = []

        result = client.discover_markets(city_codes=["NYC"])

        assert result == []

    def test_market_without_parseable_date_is_skipped(self) -> None:
        client, mock = _make_kalshi_client()
        market = _make_mock_market(ticker="BADTICKER")  # No date segment
        mock.get_markets.return_value = [market]

        result = client.discover_markets(city_codes=["NYC"])

        assert result == []

    def test_edge_bracket_below(self) -> None:
        client, mock = _make_kalshi_client()
        market = _make_mock_market(
            ticker="KXHIGHNYC-26MAR16-T49",
            subtitle="Below 50°F",
        )
        mock.get_markets.return_value = [market]

        result = client.discover_markets(
            city_codes=["NYC"], forecast_date=date(2026, 3, 16)
        )

        edges = [m for m in result if m.is_edge_bracket and m.market_type == MarketType.HIGH]
        assert len(edges) >= 1
        assert edges[0].bracket_low is None
        assert edges[0].bracket_high == Decimal("50")

    def test_edge_bracket_above(self) -> None:
        client, mock = _make_kalshi_client()
        market = _make_mock_market(
            ticker="KXHIGHNYC-26MAR16-T73",
            subtitle="73°F or above",
        )
        mock.get_markets.return_value = [market]

        result = client.discover_markets(
            city_codes=["NYC"], forecast_date=date(2026, 3, 16)
        )

        edges = [m for m in result if m.is_edge_bracket and m.market_type == MarketType.HIGH]
        assert len(edges) >= 1
        assert edges[0].bracket_low == Decimal("73")
        assert edges[0].bracket_high is None

    def test_multiple_cities(self) -> None:
        client, mock = _make_kalshi_client()

        def side_effect(*, series_ticker: str, fetch_all: bool) -> list[Any]:
            if "NYC" in series_ticker and "HIGH" in series_ticker:
                return [_make_mock_market(
                    ticker="KXHIGHNYC-26MAR16-T62",
                    subtitle="62°F to 63°F",
                )]
            if "CHI" in series_ticker and "HIGH" in series_ticker:
                return [_make_mock_market(
                    ticker="KXHIGHCHI-26MAR16-T55",
                    event_ticker="KXHIGHCHI-26MAR16",
                    subtitle="55°F to 56°F",
                )]
            return []

        mock.get_markets.side_effect = side_effect

        result = client.discover_markets(
            city_codes=["NYC", "CHI"], forecast_date=date(2026, 3, 16)
        )

        cities = {m.city_code for m in result}
        assert "NYC" in cities
        assert "CHI" in cities

    def test_auth_error_skips_series(self) -> None:
        client, mock = _make_kalshi_client()
        mock.get_markets.side_effect = AuthenticationError(401, "bad key")

        result = client.discover_markets(city_codes=["NYC"])

        assert result == []

    def test_rate_limit_skips_series(self) -> None:
        client, mock = _make_kalshi_client()
        mock.get_markets.side_effect = RateLimitError(429, "slow down")

        result = client.discover_markets(city_codes=["NYC"])

        assert result == []


# ---------------------------------------------------------------------------
# KalshiClient.fetch_snapshots tests
# ---------------------------------------------------------------------------


class TestFetchSnapshots:
    def test_fetches_single_market(self) -> None:
        client, mock = _make_kalshi_client()
        market = _make_mock_market()
        mock.get_markets.return_value = [market]

        result = client.fetch_snapshots(["KXHIGHNYC-26MAR16-T62"])

        assert len(result) == 1
        s = result[0]
        assert isinstance(s, MarketSnapshot)
        assert s.ticker == "KXHIGHNYC-26MAR16-T62"
        assert s.yes_bid == Decimal("0.50")
        assert s.yes_ask == Decimal("0.54")
        assert s.last_price == Decimal("0.52")
        assert s.volume == 150
        assert s.open_interest == 300
        assert s.timestamp.tzinfo is not None

    def test_fetches_multiple_markets(self) -> None:
        client, mock = _make_kalshi_client()
        m1 = _make_mock_market(ticker="KXHIGHNYC-26MAR16-T62")
        m2 = _make_mock_market(ticker="KXHIGHNYC-26MAR16-T63")
        mock.get_markets.return_value = [m1, m2]

        result = client.fetch_snapshots(
            ["KXHIGHNYC-26MAR16-T62", "KXHIGHNYC-26MAR16-T63"]
        )

        assert len(result) == 2

    def test_handles_none_prices(self) -> None:
        client, mock = _make_kalshi_client()
        market = _make_mock_market(
            yes_bid_dollars=None,
            yes_ask_dollars=None,
            no_bid_dollars=None,
            no_ask_dollars=None,
            volume_fp=None,
            open_interest_fp=None,
            last_price_dollars=None,
        )
        mock.get_markets.return_value = [market]

        result = client.fetch_snapshots(["KXHIGHNYC-26MAR16-T62"])

        s = result[0]
        assert s.yes_bid is None
        assert s.yes_ask is None
        assert s.volume is None
        assert s.last_price is None

    def test_batches_large_ticker_lists(self) -> None:
        client, mock = _make_kalshi_client()
        mock.get_markets.return_value = []

        # 250 tickers → should make 3 batch calls (100 + 100 + 50)
        tickers = [f"TICKER-{i}" for i in range(250)]
        client.fetch_snapshots(tickers)

        assert mock.get_markets.call_count == 3

    def test_api_error_raises_kalshi_api_error(self) -> None:
        client, mock = _make_kalshi_client()
        mock.get_markets.side_effect = KalshiAPIError(500, "server error")

        with pytest.raises(KalshiApiError, match="Failed to fetch market snapshots"):
            client.fetch_snapshots(["KXHIGHNYC-26MAR16-T62"])

    def test_auth_error_raises_kalshi_api_error(self) -> None:
        client, mock = _make_kalshi_client()
        mock.get_markets.side_effect = AuthenticationError(401, "bad key")

        with pytest.raises(KalshiApiError):
            client.fetch_snapshots(["KXHIGHNYC-26MAR16-T62"])

    @pytest.mark.parametrize(
        "raised_exc",
        [
            AuthenticationError(401, "bad key"),
            RateLimitError(429, "slow down"),
            KalshiAPIError(500, "server error"),
        ],
    )
    def test_fallback_wraps_single_ticker_errors(self, raised_exc: Exception) -> None:
        client, mock = _make_kalshi_client()
        call_count = 0

        def side_effect(*, tickers: list[str]) -> list[Any]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ResourceNotFoundError(404, "batch missing ticker")
            raise raised_exc

        mock.get_markets.side_effect = side_effect

        with pytest.raises(KalshiApiError) as excinfo:
            client.fetch_snapshots(["KXHIGHNYC-26MAR16-T62", "KXHIGHNYC-26MAR16-T63"])

        assert call_count == 2
        assert isinstance(excinfo.value.__cause__, type(raised_exc))

    def test_empty_tickers_returns_empty(self) -> None:
        client, mock = _make_kalshi_client()

        result = client.fetch_snapshots([])

        assert result == []
        mock.get_markets.assert_not_called()


# ---------------------------------------------------------------------------
# KalshiClient.check_settlements tests
# ---------------------------------------------------------------------------


class TestCheckSettlements:
    def test_finds_settled_market(self) -> None:
        client, mock = _make_kalshi_client()
        market = _make_mock_market(
            status=KalshiMarketStatus.SETTLED,
            result="yes",
            settlement_value_dollars="1.00",
        )
        mock.get_markets.return_value = [market]

        result = client.check_settlements(["KXHIGHNYC-26MAR16-T62"])

        assert len(result) == 1
        s = result[0]
        assert isinstance(s, SettledMarket)
        assert s.ticker == "KXHIGHNYC-26MAR16-T62"
        assert s.result == "yes"
        assert s.settlement_value == Decimal("1.00")
        assert s.final_status == MarketStatus.SETTLED

    def test_returns_closed_market(self) -> None:
        client, mock = _make_kalshi_client()
        market = _make_mock_market(
            status=KalshiMarketStatus.CLOSED,
            result=None,
        )
        mock.get_markets.return_value = [market]

        result = client.check_settlements(["KXHIGHNYC-26MAR16-T62"])

        assert len(result) == 1
        s = result[0]
        assert s.final_status == MarketStatus.CLOSED
        assert s.result == ""

    def test_ignores_active_markets(self) -> None:
        client, mock = _make_kalshi_client()
        active = _make_mock_market(status=KalshiMarketStatus.OPEN)
        mock.get_markets.return_value = [active]

        result = client.check_settlements(["KXHIGHNYC-26MAR16-T62"])

        assert result == []

    def test_settled_without_result_still_captured(self) -> None:
        """SETTLED with empty result is still captured to avoid zombie ACTIVE rows."""
        client, mock = _make_kalshi_client()
        market = _make_mock_market(
            status=KalshiMarketStatus.SETTLED,
            result=None,
        )
        mock.get_markets.return_value = [market]

        result = client.check_settlements(["KXHIGHNYC-26MAR16-T62"])

        assert len(result) == 1
        assert result[0].final_status == MarketStatus.SETTLED
        assert result[0].result == ""

    def test_mixed_settled_and_active(self) -> None:
        client, mock = _make_kalshi_client()
        settled = _make_mock_market(
            ticker="T1",
            status=KalshiMarketStatus.SETTLED,
            result="no",
            settlement_value_dollars="0.00",
        )
        active = _make_mock_market(ticker="T2", status=KalshiMarketStatus.OPEN)
        mock.get_markets.return_value = [settled, active]

        result = client.check_settlements(["T1", "T2"])

        assert len(result) == 1
        assert result[0].ticker == "T1"
        assert result[0].result == "no"

    def test_determined_status_counts_as_settled(self) -> None:
        client, mock = _make_kalshi_client()
        market = _make_mock_market(
            status=KalshiMarketStatus.DETERMINED,
            result="yes",
            settlement_value_dollars="1.00",
        )
        mock.get_markets.return_value = [market]

        result = client.check_settlements(["KXHIGHNYC-26MAR16-T62"])

        assert len(result) == 1

    def test_api_error_raises(self) -> None:
        client, mock = _make_kalshi_client()
        mock.get_markets.side_effect = KalshiAPIError(500, "error")

        with pytest.raises(KalshiApiError, match="Failed to check settlements"):
            client.check_settlements(["T1"])

    @pytest.mark.parametrize(
        "raised_exc",
        [
            AuthenticationError(401, "bad key"),
            RateLimitError(429, "slow down"),
            KalshiAPIError(500, "server error"),
        ],
    )
    def test_fallback_wraps_single_ticker_errors(self, raised_exc: Exception) -> None:
        client, mock = _make_kalshi_client()
        call_count = 0

        def side_effect(*, tickers: list[str]) -> list[Any]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ResourceNotFoundError(404, "batch missing ticker")
            raise raised_exc

        mock.get_markets.side_effect = side_effect

        with pytest.raises(KalshiApiError) as excinfo:
            client.check_settlements(["T1", "T2"])

        assert call_count == 2
        assert isinstance(excinfo.value.__cause__, type(raised_exc))

    def test_empty_tickers_returns_empty(self) -> None:
        client, mock = _make_kalshi_client()

        result = client.check_settlements([])

        assert result == []
        mock.get_markets.assert_not_called()


# ---------------------------------------------------------------------------
# Context manager and factory tests
# ---------------------------------------------------------------------------


class TestKalshiClientLifecycle:
    def test_context_manager_calls_close(self) -> None:
        mock_pykalshi = MagicMock()
        with KalshiClient(mock_pykalshi) as client:
            assert client is not None
        mock_pykalshi.close.assert_called_once()

    def test_close_delegates_to_pykalshi(self) -> None:
        mock_pykalshi = MagicMock()
        client = KalshiClient(mock_pykalshi)
        client.close()
        mock_pykalshi.close.assert_called_once()

    def test_from_settings_creates_client(self) -> None:
        with patch("src.clients.kalshi.PyKalshiClient") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance

            client = KalshiClient.from_settings(
                api_key_id="test-key",
                private_key_path="/path/to/key.pem",
            )

            mock_cls.assert_called_once_with(
                api_key_id="test-key",
                private_key_path="/path/to/key.pem",
            )
            assert client._client is mock_instance

    def test_from_settings_wraps_init_error(self) -> None:
        with patch("src.clients.kalshi.PyKalshiClient") as mock_cls:
            mock_cls.side_effect = FileNotFoundError("key file missing")

            with pytest.raises(KalshiApiError, match="Failed to initialize"):
                KalshiClient.from_settings(
                    api_key_id="test-key",
                    private_key_path="/nonexistent/key.pem",
                )
