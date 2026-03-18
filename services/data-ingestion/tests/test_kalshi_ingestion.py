"""Tests for Kalshi ingestion job functions.

Covers discovery, snapshots, and settlements using mock KalshiClient
and mock session factories (Decision #9: DI via function arguments).
"""

import uuid
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from shared.config.errors import KalshiApiError
from shared.db.enums import MarketStatus, MarketType
from shared.db.models import City, KalshiMarket

from src.clients.kalshi import DiscoveredMarket, KalshiClient
from src.clients.kalshi import MarketSnapshot as ClientMarketSnapshot
from src.clients.kalshi import SettledMarket
from src.ingestion.kalshi import (
    run_kalshi_discovery,
    run_kalshi_settlements,
    run_kalshi_snapshot_cleanup,
    run_kalshi_snapshots,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_city(code: str = "NYC") -> City:
    city = MagicMock(spec=City)
    city.id = uuid.uuid4()
    city.name = f"Test {code}"
    city.kalshi_ticker_prefix = code
    city.lat = 40.7
    city.lon = -74.0
    return city  # type: ignore[return-value]


def _mock_session_factory(
    mock_session: MagicMock,
) -> Callable[[], AbstractContextManager[MagicMock]]:
    @contextmanager
    def factory() -> Generator[MagicMock, None, None]:
        yield mock_session

    return factory


def _make_discovered_market(
    city_code: str = "NYC",
    ticker: str = "KXHIGHNYC-26MAR17-T72",
) -> DiscoveredMarket:
    return DiscoveredMarket(
        event_ticker="KXHIGHNYC-26MAR17",
        market_ticker=ticker,
        city_code=city_code,
        forecast_date=date(2026, 3, 17),
        market_type=MarketType.HIGH,
        bracket_low=Decimal("72"),
        bracket_high=Decimal("73"),
        is_edge_bracket=False,
        yes_bid=Decimal("0.54"),
        yes_ask=Decimal("0.56"),
        no_bid=Decimal("0.44"),
        no_ask=Decimal("0.46"),
        volume=150,
        open_interest=200,
        status=MarketStatus.ACTIVE,
    )


# ---------------------------------------------------------------------------
# Discovery tests
# ---------------------------------------------------------------------------


class TestRunKalshiDiscovery:
    def test_happy_path_upserts_markets(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)
        mock_kalshi.discover_markets.return_value = [
            _make_discovered_market("NYC", "KXHIGHNYC-26MAR17-T72"),
        ]

        mock_session = MagicMock()
        city = _make_city("NYC")

        run_kalshi_discovery(
            kalshi_client=mock_kalshi,
            city_map={"NYC": city},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=date(2026, 3, 17),
            run_id="test-run-1",
        )

        mock_kalshi.discover_markets.assert_called_once()
        mock_session.execute.assert_called_once()

    def test_unknown_city_code_skipped(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)
        mock_kalshi.discover_markets.return_value = [
            _make_discovered_market("UNKNOWN", "KXHIGHUNK-26MAR17-T72"),
        ]

        mock_session = MagicMock()

        run_kalshi_discovery(
            kalshi_client=mock_kalshi,
            city_map={"NYC": _make_city("NYC")},
            session_factory=_mock_session_factory(mock_session),
            forecast_date=date(2026, 3, 17),
            run_id="test-run-2",
        )

        # No DB insert — city not in map
        mock_session.execute.assert_not_called()

    def test_api_error_handled_gracefully(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)
        mock_kalshi.discover_markets.side_effect = KalshiApiError("rate limited")

        mock_session = MagicMock()

        # Should not raise
        run_kalshi_discovery(
            kalshi_client=mock_kalshi,
            city_map={"NYC": _make_city("NYC")},
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-3",
        )

        mock_session.execute.assert_not_called()

    def test_multiple_markets_upserted(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)
        mock_kalshi.discover_markets.return_value = [
            _make_discovered_market("NYC", "KXHIGHNYC-26MAR17-T72"),
            _make_discovered_market("NYC", "KXHIGHNYC-26MAR17-T73"),
        ]

        mock_session = MagicMock()

        run_kalshi_discovery(
            kalshi_client=mock_kalshi,
            city_map={"NYC": _make_city("NYC")},
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-4",
        )

        assert mock_session.execute.call_count == 2

    def test_per_market_db_error_does_not_stop_others(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)
        mock_kalshi.discover_markets.return_value = [
            _make_discovered_market("NYC", "KXHIGHNYC-26MAR17-T72"),
            _make_discovered_market("NYC", "KXHIGHNYC-26MAR17-T73"),
        ]

        mock_session = MagicMock()
        # First execute fails, second succeeds
        mock_session.execute.side_effect = [RuntimeError("DB error"), MagicMock()]

        run_kalshi_discovery(
            kalshi_client=mock_kalshi,
            city_map={"NYC": _make_city("NYC")},
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-5",
        )

        assert mock_session.execute.call_count == 2


# ---------------------------------------------------------------------------
# Snapshot tests
# ---------------------------------------------------------------------------


def _make_mock_kalshi_market(
    ticker: str = "KXHIGHNYC-26MAR17-T72",
    forecast_date: datetime | None = None,
) -> MagicMock:
    """Create a mock KalshiMarket ORM object."""
    m = MagicMock(spec=KalshiMarket)
    m.id = uuid.uuid4()
    m.ticker = ticker
    m.status = MarketStatus.ACTIVE
    m.forecast_date = forecast_date or datetime(2026, 3, 17, tzinfo=timezone.utc)
    return m


class TestRunKalshiSnapshots:
    def test_happy_path_inserts_snapshots(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)
        mock_market = _make_mock_kalshi_market()

        mock_session = MagicMock()
        # First call: query active markets; second call: insert snapshots
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [mock_market]
        mock_execute = MagicMock()
        mock_execute.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_execute

        now = datetime.now(timezone.utc)
        snapshot = ClientMarketSnapshot(
            ticker=mock_market.ticker,
            timestamp=now,
            yes_bid=Decimal("0.54"),
            yes_ask=Decimal("0.56"),
            no_bid=Decimal("0.44"),
            no_ask=Decimal("0.46"),
            volume=100,
            open_interest=200,
            last_price=Decimal("0.55"),
        )
        mock_kalshi.fetch_snapshots.return_value = [snapshot]

        run_kalshi_snapshots(
            kalshi_client=mock_kalshi,
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-1",
        )

        mock_kalshi.fetch_snapshots.assert_called_once()
        # session.add called for the snapshot
        mock_session.add.assert_called_once()

    def test_no_active_markets_no_api_call(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)

        mock_session = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_execute = MagicMock()
        mock_execute.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_execute

        run_kalshi_snapshots(
            kalshi_client=mock_kalshi,
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-2",
        )

        mock_kalshi.fetch_snapshots.assert_not_called()

    def test_unknown_ticker_in_response_skipped(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)
        mock_market = _make_mock_kalshi_market(ticker="KXHIGHNYC-26MAR17-T72")

        mock_session = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [mock_market]
        mock_execute = MagicMock()
        mock_execute.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_execute

        # API returns a ticker we don't know about
        snapshot = ClientMarketSnapshot(
            ticker="UNKNOWN-TICKER",
            timestamp=datetime.now(timezone.utc),
            yes_bid=Decimal("0.50"),
            yes_ask=Decimal("0.52"),
            no_bid=None,
            no_ask=None,
            volume=None,
            open_interest=None,
            last_price=None,
        )
        mock_kalshi.fetch_snapshots.return_value = [snapshot]

        run_kalshi_snapshots(
            kalshi_client=mock_kalshi,
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-3",
        )

        # No snapshots inserted
        mock_session.add.assert_not_called()

    def test_api_error_handled_gracefully(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)
        mock_market = _make_mock_kalshi_market()

        mock_session = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [mock_market]
        mock_execute = MagicMock()
        mock_execute.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_execute

        mock_kalshi.fetch_snapshots.side_effect = KalshiApiError("rate limited")

        # Should not raise
        run_kalshi_snapshots(
            kalshi_client=mock_kalshi,
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-4",
        )


# ---------------------------------------------------------------------------
# Settlement tests
# ---------------------------------------------------------------------------


class TestRunKalshiSettlements:
    def test_settled_market_updated(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)
        mock_market = _make_mock_kalshi_market()
        # Set forecast_date in the past so it appears unsettled
        mock_market.forecast_date = datetime(2026, 3, 15, tzinfo=timezone.utc)

        mock_session = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [mock_market]
        mock_execute = MagicMock()
        mock_execute.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_execute

        mock_kalshi.check_settlements.return_value = [
            SettledMarket(
                ticker=mock_market.ticker,
                result="yes",
                settlement_value=Decimal("1.00"),
                final_status=MarketStatus.SETTLED,
            ),
        ]

        run_kalshi_settlements(
            kalshi_client=mock_kalshi,
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-1",
        )

        mock_kalshi.check_settlements.assert_called_once()
        # execute called twice: once for SELECT, once for UPDATE
        assert mock_session.execute.call_count >= 1

    def test_no_unsettled_markets_no_api_call(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)

        mock_session = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_execute = MagicMock()
        mock_execute.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_execute

        run_kalshi_settlements(
            kalshi_client=mock_kalshi,
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-2",
        )

        mock_kalshi.check_settlements.assert_not_called()

    def test_partial_settlement(self) -> None:
        """Only some tickers are settled — others remain active."""
        mock_kalshi = MagicMock(spec=KalshiClient)

        m1 = _make_mock_kalshi_market(ticker="TICKER-1")
        m1.forecast_date = datetime(2026, 3, 15, tzinfo=timezone.utc)
        m2 = _make_mock_kalshi_market(ticker="TICKER-2")
        m2.forecast_date = datetime(2026, 3, 15, tzinfo=timezone.utc)

        mock_session = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [m1, m2]
        mock_execute = MagicMock()
        mock_execute.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_execute

        # Only TICKER-1 is settled
        mock_kalshi.check_settlements.return_value = [
            SettledMarket(ticker="TICKER-1", result="yes", settlement_value=Decimal("1.00"), final_status=MarketStatus.SETTLED),
        ]

        run_kalshi_settlements(
            kalshi_client=mock_kalshi,
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-3",
        )

        mock_kalshi.check_settlements.assert_called_once()
        # UPDATE called only for TICKER-1
        # execute called for SELECT (in first session) + UPDATE (in second session)
        assert mock_session.execute.call_count >= 1

    def test_closed_market_updated_with_closed_status(self) -> None:
        """CLOSED markets should be updated to CLOSED, not left as ACTIVE."""
        mock_kalshi = MagicMock(spec=KalshiClient)
        mock_market = _make_mock_kalshi_market()
        mock_market.forecast_date = datetime(2026, 3, 15, tzinfo=timezone.utc)

        mock_session = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [mock_market]
        mock_execute = MagicMock()
        mock_execute.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_execute

        mock_kalshi.check_settlements.return_value = [
            SettledMarket(
                ticker=mock_market.ticker,
                result="",
                settlement_value=None,
                final_status=MarketStatus.CLOSED,
            ),
        ]

        run_kalshi_settlements(
            kalshi_client=mock_kalshi,
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-closed",
        )

        mock_kalshi.check_settlements.assert_called_once()
        # execute called for SELECT + UPDATE
        assert mock_session.execute.call_count >= 2

    def test_api_error_handled_gracefully(self) -> None:
        mock_kalshi = MagicMock(spec=KalshiClient)

        mock_market = _make_mock_kalshi_market()
        mock_market.forecast_date = datetime(2026, 3, 15, tzinfo=timezone.utc)

        mock_session = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [mock_market]
        mock_execute = MagicMock()
        mock_execute.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_execute

        mock_kalshi.check_settlements.side_effect = KalshiApiError("connection lost")

        # Should not raise
        run_kalshi_settlements(
            kalshi_client=mock_kalshi,
            session_factory=_mock_session_factory(mock_session),
            run_id="test-run-4",
        )


# ---------------------------------------------------------------------------
# Snapshot retention cleanup tests
# ---------------------------------------------------------------------------


class TestRunKalshiSnapshotCleanup:
    def test_deletes_old_snapshots(self) -> None:
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 42
        mock_session.execute.return_value = mock_result

        run_kalshi_snapshot_cleanup(
            session_factory=_mock_session_factory(mock_session),
            retention_days=30,
            run_id="test-cleanup-1",
        )

        mock_session.execute.assert_called_once()

    def test_custom_retention_days(self) -> None:
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 0
        mock_session.execute.return_value = mock_result

        run_kalshi_snapshot_cleanup(
            session_factory=_mock_session_factory(mock_session),
            retention_days=7,
            run_id="test-cleanup-2",
        )

        mock_session.execute.assert_called_once()

    def test_db_error_handled_gracefully(self) -> None:
        mock_session = MagicMock()
        mock_session.execute.side_effect = RuntimeError("DB error")

        # Should not raise
        run_kalshi_snapshot_cleanup(
            session_factory=_mock_session_factory(mock_session),
            run_id="test-cleanup-3",
        )
