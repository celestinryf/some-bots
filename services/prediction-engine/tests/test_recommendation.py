"""Tests for engine/recommendation.py — recommendation cycle orchestrator.

All DB access is mocked via session_factory injection.  Tests cover
candidate selection, gap/EV filtering, risk scoring, recommendation
upsert, paper trade creation, and error isolation.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from src.config import PredictionConfig
from src.engine.decimal_utils import decimal_from_json
from src.engine.recommendation import (
    _build_candidate,
    _get_prediction_probability,
    _is_valid_price,
    _select_best_candidate,
    run_recommendation_cycle,
)
from tests.factories import (
    make_market,
    make_paper_trade,
    make_prediction,
    make_recommendation,
    make_snapshot,
)

from shared.config.errors import RecommendationError
from shared.db.enums import Direction, MarketStatus, MarketType

_CITY = uuid.UUID("00000000-0000-0000-0000-000000000001")
_DATE = datetime(2026, 3, 20, tzinfo=UTC)
_NOW = datetime(2026, 3, 19, 18, 0, tzinfo=UTC)


def _config(**overrides: object) -> PredictionConfig:
    defaults = {
        "min_sources_required": 2,
        "std_dev_floor": Decimal("1.50"),
    }
    defaults.update(overrides)
    return PredictionConfig(**defaults)  # type: ignore[arg-type]


def _mock_session_factory(session: MagicMock):
    @contextmanager
    def _factory():
        yield session

    return _factory


_REC_MODULE = "src.engine.recommendation"


# ---------------------------------------------------------------------------
# decimal_from_json (shared utility)
# ---------------------------------------------------------------------------

_SRC = "test"


class TestDecimalFromJson:
    def test_decimal_passthrough(self) -> None:
        assert decimal_from_json(Decimal("1.5"), source=_SRC) == Decimal("1.5")

    def test_int(self) -> None:
        assert decimal_from_json(42, source=_SRC) == Decimal("42")

    def test_float(self) -> None:
        assert decimal_from_json(0.45, source=_SRC) == Decimal("0.45")

    def test_string(self) -> None:
        assert decimal_from_json("0.123", source=_SRC) == Decimal("0.123")

    def test_invalid_string(self) -> None:
        with pytest.raises(Exception, match="Invalid decimal"):
            decimal_from_json("not_a_number", source=_SRC)

    def test_unsupported_type(self) -> None:
        with pytest.raises(Exception, match="Unsupported"):
            decimal_from_json([1, 2, 3], source=_SRC)


# ---------------------------------------------------------------------------
# _is_valid_price
# ---------------------------------------------------------------------------


class TestIsValidPrice:
    @pytest.mark.parametrize(
        "price,expected",
        [
            (Decimal("0.50"), True),
            (Decimal("0.01"), True),
            (Decimal("0.99"), True),
            (Decimal("0"), False),
            (Decimal("1"), False),
            (Decimal("-0.10"), False),
            (Decimal("1.50"), False),
            (None, False),
        ],
    )
    def test_valid_prices(self, price, expected) -> None:
        assert _is_valid_price(price) is expected


# ---------------------------------------------------------------------------
# _get_prediction_probability
# ---------------------------------------------------------------------------


class TestGetPredictionProbability:
    def test_extracts_probability(self) -> None:
        prediction = make_prediction(
            probability_distribution={
                "brackets": {"[65.0000, 70.0000)": 0.45},
                "mean": 68.5,
                "std_dev": 2.0,
                "source_temps": {"NWS": 68.0},
                "sum_check": 1.0,
            }
        )
        market = make_market(
            bracket_low=Decimal("65.0000"),
            bracket_high=Decimal("70.0000"),
        )

        prob = _get_prediction_probability(prediction, market)
        assert prob == Decimal("0.4500")

    def test_missing_brackets_key_raises(self) -> None:
        prediction = make_prediction(probability_distribution={"mean": 68.5})
        market = make_market()

        with pytest.raises(RecommendationError, match="missing bracket"):
            _get_prediction_probability(prediction, market)

    def test_missing_specific_bracket_raises(self) -> None:
        prediction = make_prediction(
            probability_distribution={
                "brackets": {"[70.0000, 75.0000)": 0.30},
            }
        )
        market = make_market(
            bracket_low=Decimal("65.0000"),
            bracket_high=Decimal("70.0000"),
        )

        with pytest.raises(RecommendationError, match="no probability"):
            _get_prediction_probability(prediction, market)

    def test_invalid_probability_raises(self) -> None:
        prediction = make_prediction(
            probability_distribution={
                "brackets": {"[65.0000, 70.0000)": 1.5},
            }
        )
        market = make_market(
            bracket_low=Decimal("65.0000"),
            bracket_high=Decimal("70.0000"),
        )

        with pytest.raises(RecommendationError, match="invalid"):
            _get_prediction_probability(prediction, market)

    def test_none_distribution_raises(self) -> None:
        prediction = make_prediction()
        prediction.probability_distribution = None
        market = make_market()

        with pytest.raises(RecommendationError, match="missing bracket"):
            _get_prediction_probability(prediction, market)


# ---------------------------------------------------------------------------
# _build_candidate
# ---------------------------------------------------------------------------


class TestBuildCandidate:
    def test_buy_yes_candidate(self) -> None:
        c = _build_candidate(
            Direction.BUY_YES,
            model_win_probability=Decimal("0.60"),
            entry_price=Decimal("0.40"),
        )
        assert c.direction == Direction.BUY_YES
        assert c.model_probability == Decimal("0.6000")
        assert c.kalshi_probability == Decimal("0.4000")
        assert c.gap == Decimal("0.2000")
        # EV should be positive since model_prob > entry_price
        assert c.expected_value > 0


# ---------------------------------------------------------------------------
# _select_best_candidate
# ---------------------------------------------------------------------------


class TestSelectBestCandidate:
    def test_buy_yes_above_threshold(self) -> None:
        """Model says 60%, market asks 35% → gap 25% > 20% threshold."""
        snapshot = make_snapshot(
            yes_ask=Decimal("0.3500"),
            no_ask=Decimal("0.7000"),
        )
        config = _config(
            gap_threshold=Decimal("0.20"),
            min_ev_threshold=Decimal("0.01"),
        )

        candidate = _select_best_candidate(
            model_probability=Decimal("0.60"),
            snapshot=snapshot,
            config=config,
        )

        assert candidate is not None
        assert candidate.direction == Direction.BUY_YES

    def test_buy_no_selected(self) -> None:
        """Model says 30% YES → 70% NO, market asks 40% NO → gap 30%."""
        snapshot = make_snapshot(
            yes_ask=Decimal("0.7500"),  # Too expensive for YES
            no_ask=Decimal("0.4000"),
        )
        config = _config(
            gap_threshold=Decimal("0.20"),
            min_ev_threshold=Decimal("0.01"),
        )

        candidate = _select_best_candidate(
            model_probability=Decimal("0.30"),
            snapshot=snapshot,
            config=config,
        )

        assert candidate is not None
        assert candidate.direction == Direction.BUY_NO

    def test_no_candidate_below_threshold(self) -> None:
        """Both directions below gap threshold → None."""
        snapshot = make_snapshot(
            yes_ask=Decimal("0.4500"),
            no_ask=Decimal("0.5500"),
        )
        config = _config(
            gap_threshold=Decimal("0.20"),
            min_ev_threshold=Decimal("0.01"),
        )

        candidate = _select_best_candidate(
            model_probability=Decimal("0.50"),
            snapshot=snapshot,
            config=config,
        )

        assert candidate is None

    def test_none_prices_excluded(self) -> None:
        """Snapshot with None ask prices → no candidates."""
        snapshot = make_snapshot(yes_ask=None, no_ask=None)
        config = _config(
            gap_threshold=Decimal("0.01"),
            min_ev_threshold=Decimal("0.001"),
        )

        candidate = _select_best_candidate(
            model_probability=Decimal("0.50"),
            snapshot=snapshot,
            config=config,
        )

        assert candidate is None

    def test_best_ev_wins(self) -> None:
        """When both directions eligible, higher EV wins."""
        snapshot = make_snapshot(
            yes_ask=Decimal("0.2000"),
            no_ask=Decimal("0.2000"),
        )
        config = _config(
            gap_threshold=Decimal("0.01"),
            min_ev_threshold=Decimal("0.001"),
        )

        # model_probability = 0.80 → YES gap = 0.60, NO gap = 0.00
        candidate = _select_best_candidate(
            model_probability=Decimal("0.80"),
            snapshot=snapshot,
            config=config,
        )

        assert candidate is not None
        assert candidate.direction == Direction.BUY_YES


# ---------------------------------------------------------------------------
# run_recommendation_cycle — full orchestration
# ---------------------------------------------------------------------------


class TestRecommendationCycleHappyPath:
    @patch(f"{_REC_MODULE}.get_or_create_paper_trade")
    @patch(f"{_REC_MODULE}.get_or_create_recommendation")
    @patch(f"{_REC_MODULE}.fetch_latest_snapshot_map")
    @patch(f"{_REC_MODULE}.fetch_predictions_for_model")
    def test_happy_path(
        self,
        mock_predictions,
        mock_snapshots,
        mock_rec_upsert,
        mock_trade_upsert,
    ) -> None:
        market = make_market(
            city_id=_CITY,
            bracket_low=Decimal("65.0000"),
            bracket_high=Decimal("70.0000"),
            status=MarketStatus.ACTIVE,
        )
        market.forecast_date = _DATE
        market.market_type = MarketType.HIGH

        prediction = make_prediction(
            city_id=_CITY,
            predicted_temp=Decimal("68.50"),
            std_dev=Decimal("2.00"),
            probability_distribution={
                "brackets": {"[65.0000, 70.0000)": 0.65},
                "mean": 68.5,
                "std_dev": 2.0,
                "source_temps": {"NWS": 68.0, "VC": 69.0},
                "sum_check": 1.0,
            },
        )

        snapshot = make_snapshot(
            market_id=market.id,
            yes_ask=Decimal("0.3500"),
            no_ask=Decimal("0.7000"),
            volume=100,
        )

        mock_predictions.return_value = {
            (_CITY, _DATE, MarketType.HIGH): prediction,
        }
        mock_snapshots.return_value = {market.id: snapshot}

        rec = make_recommendation()
        mock_rec_upsert.return_value = (rec, True)
        trade = make_paper_trade()
        mock_trade_upsert.return_value = (trade, True)

        # Mock session with market query
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [
            market,
        ]

        config = _config(
            gap_threshold=Decimal("0.20"),
            min_ev_threshold=Decimal("0.01"),
        )

        stats = run_recommendation_cycle(
            config,
            _mock_session_factory(session),
            now_fn=lambda: _NOW,
        )

        assert stats["markets_seen"] == 1
        assert stats["recommendations_created"] == 1
        assert stats["paper_trades_created"] == 1
        assert stats["markets_skipped"] == 0
        assert stats["markets_errored"] == 0


class TestRecommendationCycleSkipping:
    @patch(f"{_REC_MODULE}.fetch_latest_snapshot_map")
    @patch(f"{_REC_MODULE}.fetch_predictions_for_model")
    def test_missing_prediction_skips(
        self, mock_predictions, mock_snapshots
    ) -> None:
        market = make_market(city_id=_CITY, status=MarketStatus.ACTIVE)
        market.forecast_date = _DATE
        market.market_type = MarketType.HIGH

        mock_predictions.return_value = {}  # No predictions
        mock_snapshots.return_value = {
            market.id: make_snapshot(market_id=market.id),
        }

        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [
            market,
        ]

        stats = run_recommendation_cycle(
            _config(),
            _mock_session_factory(session),
            now_fn=lambda: _NOW,
        )

        assert stats["markets_skipped"] == 1
        assert stats["recommendations_created"] == 0

    @patch(f"{_REC_MODULE}.fetch_latest_snapshot_map")
    @patch(f"{_REC_MODULE}.fetch_predictions_for_model")
    def test_missing_snapshot_skips(
        self, mock_predictions, mock_snapshots
    ) -> None:
        market = make_market(city_id=_CITY, status=MarketStatus.ACTIVE)
        market.forecast_date = _DATE
        market.market_type = MarketType.HIGH

        prediction = make_prediction(city_id=_CITY)
        prediction.forecast_date = _DATE
        prediction.market_type = MarketType.HIGH
        mock_predictions.return_value = {
            (_CITY, _DATE, MarketType.HIGH): prediction,
        }
        mock_snapshots.return_value = {}  # No snapshots

        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [
            market,
        ]

        stats = run_recommendation_cycle(
            _config(),
            _mock_session_factory(session),
            now_fn=lambda: _NOW,
        )

        assert stats["markets_skipped"] == 1

    @patch(f"{_REC_MODULE}.get_or_create_paper_trade")
    @patch(f"{_REC_MODULE}.get_or_create_recommendation")
    @patch(f"{_REC_MODULE}.fetch_latest_snapshot_map")
    @patch(f"{_REC_MODULE}.fetch_predictions_for_model")
    def test_below_threshold_skips(
        self,
        mock_predictions,
        mock_snapshots,
        mock_rec_upsert,
        mock_trade_upsert,
    ) -> None:
        """Market+model agree → gap below threshold → skip."""
        market = make_market(
            city_id=_CITY,
            bracket_low=Decimal("65.0000"),
            bracket_high=Decimal("70.0000"),
            status=MarketStatus.ACTIVE,
        )
        market.forecast_date = _DATE
        market.market_type = MarketType.HIGH

        prediction = make_prediction(
            city_id=_CITY,
            probability_distribution={
                "brackets": {"[65.0000, 70.0000)": 0.45},
                "mean": 68.5,
                "std_dev": 2.0,
                "source_temps": {"NWS": 68.0},
                "sum_check": 1.0,
            },
        )

        # Market agrees: yes_ask ≈ 0.45 → gap ≈ 0
        snapshot = make_snapshot(
            market_id=market.id,
            yes_ask=Decimal("0.4400"),
            no_ask=Decimal("0.5600"),
        )

        mock_predictions.return_value = {
            (_CITY, _DATE, MarketType.HIGH): prediction,
        }
        mock_snapshots.return_value = {market.id: snapshot}

        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [
            market,
        ]

        config = _config(
            gap_threshold=Decimal("0.20"),
            min_ev_threshold=Decimal("0.08"),
        )

        stats = run_recommendation_cycle(
            config,
            _mock_session_factory(session),
            now_fn=lambda: _NOW,
        )

        assert stats["markets_skipped"] == 1
        assert stats["recommendations_created"] == 0

    @patch(f"{_REC_MODULE}.fetch_latest_snapshot_map")
    @patch(f"{_REC_MODULE}.fetch_predictions_for_model")
    def test_no_markets(self, mock_predictions, mock_snapshots) -> None:
        mock_predictions.return_value = {}
        mock_snapshots.return_value = {}

        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = []

        stats = run_recommendation_cycle(
            _config(),
            _mock_session_factory(session),
            now_fn=lambda: _NOW,
        )

        assert stats["markets_seen"] == 0


class TestRecommendationCycleErrorIsolation:
    @patch(f"{_REC_MODULE}.get_or_create_paper_trade")
    @patch(f"{_REC_MODULE}.get_or_create_recommendation")
    @patch(f"{_REC_MODULE}.fetch_latest_snapshot_map")
    @patch(f"{_REC_MODULE}.fetch_predictions_for_model")
    def test_error_in_one_market_continues(
        self,
        mock_predictions,
        mock_snapshots,
        mock_rec_upsert,
        mock_trade_upsert,
    ) -> None:
        """Error processing one market should not block the next."""
        m1 = make_market(
            city_id=_CITY,
            bracket_low=Decimal("65.0000"),
            bracket_high=Decimal("70.0000"),
            status=MarketStatus.ACTIVE,
        )
        m2 = make_market(
            city_id=_CITY,
            bracket_low=Decimal("70.0000"),
            bracket_high=Decimal("75.0000"),
            status=MarketStatus.ACTIVE,
        )
        m1.forecast_date = _DATE
        m2.forecast_date = _DATE
        m1.market_type = MarketType.HIGH
        m2.market_type = MarketType.HIGH

        # m1: bad prediction (missing bracket probability)
        p1 = make_prediction(
            city_id=_CITY,
            probability_distribution={
                "brackets": {},  # Missing bracket key → RecommendationError
            },
        )
        s1 = make_snapshot(
            market_id=m1.id,
            yes_ask=Decimal("0.3500"),
            no_ask=Decimal("0.7000"),
            volume=100,
        )
        s2 = make_snapshot(
            market_id=m2.id,
            yes_ask=Decimal("0.3500"),
            no_ask=Decimal("0.7000"),
            volume=100,
        )

        mock_predictions.return_value = {
            (_CITY, _DATE, MarketType.HIGH): p1,
        }
        # Override: need different predictions for different markets.
        # The cycle uses the same prediction for both markets (same city/date/type).
        # m1 will fail because the bracket key doesn't exist.
        # m2 will also use the same prediction (p1), so it will also fail.
        # Let's use a prediction with m2's bracket but not m1's bracket.
        p_shared = make_prediction(
            city_id=_CITY,
            probability_distribution={
                "brackets": {"[70.0000, 75.0000)": 0.65},
                "mean": 72.0,
                "std_dev": 2.0,
                "source_temps": {"NWS": 72.0},
                "sum_check": 1.0,
            },
        )
        mock_predictions.return_value = {
            (_CITY, _DATE, MarketType.HIGH): p_shared,
        }
        mock_snapshots.return_value = {m1.id: s1, m2.id: s2}

        rec = make_recommendation()
        mock_rec_upsert.return_value = (rec, True)
        trade = make_paper_trade()
        mock_trade_upsert.return_value = (trade, True)

        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [
            m1,
            m2,
        ]

        config = _config(
            gap_threshold=Decimal("0.20"),
            min_ev_threshold=Decimal("0.01"),
        )

        stats = run_recommendation_cycle(
            config,
            _mock_session_factory(session),
            now_fn=lambda: _NOW,
        )

        # m1 should error (missing bracket "[65.0000, 70.0000)"),
        # m2 should succeed
        assert stats["markets_seen"] == 2
        assert stats["markets_errored"] == 1
        assert stats["recommendations_created"] == 1


class TestRecommendationCycleReuse:
    @patch(f"{_REC_MODULE}.get_or_create_paper_trade")
    @patch(f"{_REC_MODULE}.get_or_create_recommendation")
    @patch(f"{_REC_MODULE}.fetch_latest_snapshot_map")
    @patch(f"{_REC_MODULE}.fetch_predictions_for_model")
    def test_reused_recommendation(
        self,
        mock_predictions,
        mock_snapshots,
        mock_rec_upsert,
        mock_trade_upsert,
    ) -> None:
        """Existing recommendation: counts as reused, not created."""
        market = make_market(
            city_id=_CITY,
            bracket_low=Decimal("65.0000"),
            bracket_high=Decimal("70.0000"),
            status=MarketStatus.ACTIVE,
        )
        market.forecast_date = _DATE
        market.market_type = MarketType.HIGH

        prediction = make_prediction(
            city_id=_CITY,
            probability_distribution={
                "brackets": {"[65.0000, 70.0000)": 0.65},
                "mean": 68.5,
                "std_dev": 2.0,
                "source_temps": {"NWS": 68.0},
                "sum_check": 1.0,
            },
        )

        snapshot = make_snapshot(
            market_id=market.id,
            yes_ask=Decimal("0.3500"),
            no_ask=Decimal("0.7000"),
            volume=100,
        )

        mock_predictions.return_value = {
            (_CITY, _DATE, MarketType.HIGH): prediction,
        }
        mock_snapshots.return_value = {market.id: snapshot}

        rec = make_recommendation()
        mock_rec_upsert.return_value = (rec, False)  # Existing
        trade = make_paper_trade()
        mock_trade_upsert.return_value = (trade, False)  # Existing

        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [
            market,
        ]

        config = _config(
            gap_threshold=Decimal("0.20"),
            min_ev_threshold=Decimal("0.01"),
        )

        stats = run_recommendation_cycle(
            config,
            _mock_session_factory(session),
            now_fn=lambda: _NOW,
        )

        assert stats["recommendations_created"] == 0
        assert stats["recommendations_reused"] == 1
        assert stats["paper_trades_created"] == 0
