"""Tests for data/queries.py — bulk DB query and get-or-create helpers.

Uses MagicMock(spec=Session) to test query construction and result handling
without a real database.  Integration tests for PostgreSQL-specific features
(window functions, JSONB) should use @pytest.mark.db against a real DB.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from src.data.queries import (
    fetch_active_market_groups,
    fetch_all_source_temperatures,
    fetch_markets_by_ids,
    fetch_predictions_for_model,
    fetch_source_temperatures,
    get_or_create_paper_trade,
    get_or_create_prediction,
    get_or_create_recommendation,
)
from src.engine.types import PredictionGroup
from tests.factories import make_forecast, make_market, make_prediction

from shared.db.enums import MarketType

_CITY_A = uuid.UUID("00000000-0000-0000-0000-000000000001")
_CITY_B = uuid.UUID("00000000-0000-0000-0000-000000000002")
_DATE = datetime(2026, 3, 20, tzinfo=UTC)


def _mock_session() -> MagicMock:
    """Return a MagicMock with spec=Session."""
    return MagicMock(spec=Session)


# ---------------------------------------------------------------------------
# fetch_source_temperatures
# ---------------------------------------------------------------------------


class TestFetchSourceTemperatures:
    def test_returns_latest_per_source_high(self) -> None:
        """Two NWS forecasts — should pick the first (ordered by issued_at desc)."""
        nws_latest = make_forecast(
            source="NWS",
            temp_high=Decimal("72.00"),
            issued_at=datetime(2026, 3, 19, 18, 0, tzinfo=UTC),
        )
        nws_old = make_forecast(
            source="NWS",
            temp_high=Decimal("70.00"),
            issued_at=datetime(2026, 3, 19, 6, 0, tzinfo=UTC),
        )
        vc = make_forecast(
            source="VC",
            temp_high=Decimal("74.00"),
            issued_at=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
        )

        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = [
            nws_latest,
            nws_old,
            vc,
        ]

        result = fetch_source_temperatures(
            session,
            city_id=_CITY_A,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
        )

        assert result == {"NWS": Decimal("72.00"), "VC": Decimal("74.00")}

    def test_returns_temp_low_for_low_market(self) -> None:
        nws = make_forecast(
            source="NWS",
            temp_high=Decimal("72.00"),
            temp_low=Decimal("55.00"),
        )
        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = [nws]

        result = fetch_source_temperatures(
            session,
            city_id=_CITY_A,
            forecast_date=_DATE,
            market_type=MarketType.LOW,
        )

        assert result == {"NWS": Decimal("55.00")}

    def test_skips_none_temps(self) -> None:
        """Source with NULL temp_high is excluded."""
        nws = make_forecast(source="NWS", temp_high=None)
        vc = make_forecast(source="VC", temp_high=Decimal("70.00"))

        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = [
            nws,
            vc,
        ]

        result = fetch_source_temperatures(
            session,
            city_id=_CITY_A,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
        )

        assert result == {"VC": Decimal("70.00")}

    def test_empty_forecasts(self) -> None:
        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = []

        result = fetch_source_temperatures(
            session,
            city_id=_CITY_A,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
        )

        assert result == {}


# ---------------------------------------------------------------------------
# fetch_all_source_temperatures
# ---------------------------------------------------------------------------


class TestFetchAllSourceTemperatures:
    def test_delegates_per_group(self) -> None:
        groups = [
            PredictionGroup(
                city_id=_CITY_A,
                forecast_date=_DATE,
                market_type=MarketType.HIGH,
                market_ids=(uuid.uuid4(),),
            ),
            PredictionGroup(
                city_id=_CITY_B,
                forecast_date=_DATE,
                market_type=MarketType.LOW,
                market_ids=(uuid.uuid4(),),
            ),
        ]

        with patch(
            "src.data.queries.fetch_source_temperatures"
        ) as mock_fetch:
            mock_fetch.side_effect = [
                {"NWS": Decimal("72.00")},
                {"VC": Decimal("55.00")},
            ]
            session = _mock_session()
            result = fetch_all_source_temperatures(session, groups=groups)

        assert len(result) == 2
        assert result[(_CITY_A, _DATE, MarketType.HIGH)] == {
            "NWS": Decimal("72.00")
        }
        assert result[(_CITY_B, _DATE, MarketType.LOW)] == {
            "VC": Decimal("55.00")
        }
        assert mock_fetch.call_count == 2


# ---------------------------------------------------------------------------
# fetch_active_market_groups
# ---------------------------------------------------------------------------


class TestFetchActiveMarketGroups:
    def test_groups_markets_by_city_date_type(self) -> None:
        m1 = make_market(
            city_id=_CITY_A,
            bracket_low=Decimal("65.0000"),
            bracket_high=Decimal("70.0000"),
        )
        m2 = make_market(
            city_id=_CITY_A,
            bracket_low=Decimal("70.0000"),
            bracket_high=Decimal("75.0000"),
        )
        # Force same forecast_date and market_type
        m1.forecast_date = _DATE
        m2.forecast_date = _DATE
        m1.market_type = MarketType.HIGH
        m2.market_type = MarketType.HIGH
        m1.city_id = _CITY_A
        m2.city_id = _CITY_A

        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = [
            m1,
            m2,
        ]

        groups = fetch_active_market_groups(session)

        assert len(groups) == 1
        assert groups[0].city_id == _CITY_A
        assert len(groups[0].market_ids) == 2

    def test_empty_markets_returns_empty(self) -> None:
        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = []

        groups = fetch_active_market_groups(session)
        assert groups == []

    def test_multiple_cities_create_separate_groups(self) -> None:
        m1 = make_market(city_id=_CITY_A)
        m2 = make_market(city_id=_CITY_B)
        m1.forecast_date = _DATE
        m2.forecast_date = _DATE
        m1.market_type = MarketType.HIGH
        m2.market_type = MarketType.HIGH

        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = [
            m1,
            m2,
        ]

        groups = fetch_active_market_groups(session)

        assert len(groups) == 2
        city_ids = {g.city_id for g in groups}
        assert city_ids == {_CITY_A, _CITY_B}

    def test_sorts_brackets_within_group(self) -> None:
        """Markets within a group should be sorted by bracket bounds."""
        m_high = make_market(
            city_id=_CITY_A,
            bracket_low=Decimal("70.0000"),
            bracket_high=Decimal("75.0000"),
        )
        m_low = make_market(
            city_id=_CITY_A,
            bracket_low=Decimal("60.0000"),
            bracket_high=Decimal("65.0000"),
        )
        m_high.forecast_date = _DATE
        m_low.forecast_date = _DATE
        m_high.market_type = MarketType.HIGH
        m_low.market_type = MarketType.HIGH

        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = [
            m_high,
            m_low,
        ]

        groups = fetch_active_market_groups(session)

        assert len(groups) == 1
        # m_low should be first (lower bracket_low)
        assert groups[0].market_ids[0] == m_low.id
        assert groups[0].market_ids[1] == m_high.id


# ---------------------------------------------------------------------------
# fetch_markets_by_ids
# ---------------------------------------------------------------------------


class TestFetchMarketsByIds:
    def test_returns_markets_in_input_order(self) -> None:
        m1 = make_market()
        m2 = make_market()
        ids = (m2.id, m1.id)

        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = [
            m1,
            m2,
        ]

        result = fetch_markets_by_ids(session, ids)

        assert result[0].id == m2.id
        assert result[1].id == m1.id

    def test_empty_ids_returns_empty(self) -> None:
        session = _mock_session()
        result = fetch_markets_by_ids(session, ())
        assert result == []
        session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# fetch_predictions_for_model
# ---------------------------------------------------------------------------


class TestFetchPredictionsForModel:
    def test_returns_keyed_dict(self) -> None:
        p1 = make_prediction(city_id=_CITY_A)
        p1.forecast_date = _DATE
        p1.market_type = MarketType.HIGH

        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = [
            p1,
        ]

        result = fetch_predictions_for_model(session, "tier1_equal_weight_v1")

        assert (_CITY_A, _DATE, MarketType.HIGH) in result
        assert result[(_CITY_A, _DATE, MarketType.HIGH)] is p1

    def test_empty_predictions(self) -> None:
        session = _mock_session()
        session.execute.return_value.scalars.return_value.all.return_value = []

        result = fetch_predictions_for_model(session, "tier1_equal_weight_v1")
        assert result == {}


# ---------------------------------------------------------------------------
# get_or_create_prediction
# ---------------------------------------------------------------------------


class TestGetOrCreatePrediction:
    def test_returns_existing(self) -> None:
        existing = make_prediction()
        session = _mock_session()
        session.execute.return_value.scalars.return_value.first.return_value = (
            existing
        )

        prediction, created = get_or_create_prediction(
            session,
            city_id=_CITY_A,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
            model_version="tier1_equal_weight_v1",
        )

        assert prediction is existing
        assert created is False
        session.add.assert_not_called()

    def test_creates_new(self) -> None:
        session = _mock_session()
        session.execute.return_value.scalars.return_value.first.return_value = (
            None
        )

        prediction, created = get_or_create_prediction(
            session,
            city_id=_CITY_A,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
            model_version="tier1_equal_weight_v1",
        )

        assert created is True
        session.add.assert_called_once()

    def test_integrity_error_recovery(self) -> None:
        """Concurrent insert: IntegrityError on flush, re-query succeeds."""
        existing = make_prediction()
        session = _mock_session()
        # First query: nothing found
        session.execute.return_value.scalars.return_value.first.return_value = (
            None
        )
        # begin_nested raises IntegrityError on __enter__
        session.begin_nested.return_value.__enter__ = MagicMock()
        session.flush.side_effect = IntegrityError(
            "duplicate", params=None, orig=Exception()
        )

        # After IntegrityError, re-query returns the existing row
        session.execute.return_value.scalar_one.return_value = existing

        # The function uses begin_nested as a context manager, then flush.
        # We need the context manager to work but flush to raise.
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=None)
        ctx.__exit__ = MagicMock(return_value=False)
        session.begin_nested.return_value = ctx

        # Simulate: flush raises IntegrityError
        session.flush.side_effect = IntegrityError(
            "duplicate", params=None, orig=Exception()
        )

        # When flush raises inside the context manager, __exit__ propagates
        # We need to simulate the whole flow differently.
        # Instead, let's mock begin_nested to raise on __enter__
        err = IntegrityError("duplicate", params=None, orig=Exception())

        def raise_on_enter():
            raise err

        ctx.__exit__ = MagicMock(return_value=False)

        # Actually, the simplest approach: mock begin_nested to yield a
        # context that raises IntegrityError when exiting
        session.begin_nested.side_effect = None
        session.begin_nested.return_value = ctx

        # This is getting complex. Let's take a simpler approach:
        # Mock the whole flow at a higher level. The function catches
        # IntegrityError from the `with session.begin_nested()` block.
        # We can make session.add + session.flush raise inside that block.

        # Reset and use a side_effect on begin_nested's __enter__
        session2 = _mock_session()
        # First call: nothing found
        first_result = MagicMock()
        first_result.scalars.return_value.first.return_value = None
        # Second call (after IntegrityError): existing found
        second_result = MagicMock()
        second_result.scalar_one.return_value = existing

        session2.execute.side_effect = [first_result, second_result]

        # Make begin_nested context raise IntegrityError
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=None)
        cm.__exit__ = MagicMock(return_value=False)
        session2.begin_nested.return_value = cm

        session2.flush.side_effect = IntegrityError(
            "dup", params=None, orig=Exception()
        )

        prediction, created = get_or_create_prediction(
            session2,
            city_id=_CITY_A,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
            model_version="tier1_equal_weight_v1",
        )

        assert prediction is existing
        assert created is False


# ---------------------------------------------------------------------------
# get_or_create_recommendation
# ---------------------------------------------------------------------------


class TestGetOrCreateRecommendation:
    def test_returns_existing(self) -> None:
        from tests.factories import make_recommendation

        existing = make_recommendation()
        session = _mock_session()
        session.execute.return_value.scalars.return_value.first.return_value = (
            existing
        )

        rec, created = get_or_create_recommendation(
            session,
            prediction_id=uuid.uuid4(),
            market_id=uuid.uuid4(),
        )

        assert rec is existing
        assert created is False

    def test_creates_new(self) -> None:
        session = _mock_session()
        session.execute.return_value.scalars.return_value.first.return_value = (
            None
        )

        rec, created = get_or_create_recommendation(
            session,
            prediction_id=uuid.uuid4(),
            market_id=uuid.uuid4(),
        )

        assert created is True
        session.add.assert_called_once()


# ---------------------------------------------------------------------------
# get_or_create_paper_trade
# ---------------------------------------------------------------------------


class TestGetOrCreatePaperTrade:
    def test_returns_existing(self) -> None:
        from tests.factories import make_paper_trade

        existing = make_paper_trade()
        session = _mock_session()
        session.execute.return_value.scalar_one_or_none.return_value = existing

        trade, created = get_or_create_paper_trade(
            session,
            recommendation_id=uuid.uuid4(),
            entry_price=Decimal("0.4500"),
        )

        assert trade is existing
        assert created is False

    def test_creates_new(self) -> None:
        session = _mock_session()
        session.execute.return_value.scalar_one_or_none.return_value = None

        trade, created = get_or_create_paper_trade(
            session,
            recommendation_id=uuid.uuid4(),
            entry_price=Decimal("0.4500"),
        )

        assert created is True
        session.add.assert_called_once()
