"""Tests for engine/prediction.py — prediction cycle orchestrator.

All DB access is mocked via session_factory injection.  Tests cover
orchestration logic: group iteration, source loading, model dispatch,
distribution building, prediction upsert, and error isolation.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

from src.config import PredictionConfig
from src.engine.prediction import run_prediction_cycle
from src.engine.types import PredictionGroup
from tests.factories import make_market, make_prediction

from shared.config.errors import PredictionError
from shared.db.enums import MarketType

_CITY = uuid.UUID("00000000-0000-0000-0000-000000000001")
_DATE = datetime(2026, 3, 20, tzinfo=UTC)


def _config(**overrides: object) -> PredictionConfig:
    defaults = {
        "min_sources_required": 2,
        "std_dev_floor": Decimal("1.50"),
    }
    defaults.update(overrides)
    return PredictionConfig(**defaults)  # type: ignore[arg-type]


def _mock_session_factory(session: MagicMock):
    """Build a session_factory that yields the given mock session."""

    @contextmanager
    def _factory():
        yield session

    return _factory


_QUERY_MODULE = "src.engine.prediction"


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestPredictionCycleHappyPath:
    @patch(f"{_QUERY_MODULE}.build_probability_distribution")
    @patch(f"{_QUERY_MODULE}.get_or_create_prediction")
    @patch(f"{_QUERY_MODULE}.fetch_markets_by_ids")
    @patch(f"{_QUERY_MODULE}.fetch_all_source_temperatures")
    @patch(f"{_QUERY_MODULE}.fetch_active_market_groups")
    def test_single_group_prediction(
        self,
        mock_groups,
        mock_temps,
        mock_markets,
        mock_upsert,
        mock_dist,
    ) -> None:
        market_id = uuid.uuid4()
        group = PredictionGroup(
            city_id=_CITY,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
            market_ids=(market_id,),
        )
        mock_groups.return_value = [group]
        mock_temps.return_value = {
            (_CITY, _DATE, MarketType.HIGH): {
                "NWS": Decimal("70.00"),
                "VC": Decimal("72.00"),
            },
        }
        market = make_market(
            market_id=market_id,
            bracket_low=Decimal("65.0000"),
            bracket_high=Decimal("70.0000"),
        )
        mock_markets.return_value = [market]

        prediction = make_prediction()
        mock_upsert.return_value = (prediction, True)

        mock_dist.return_value = {
            "brackets": {"[65.0000, 70.0000)": 0.35},
            "mean": 71.0,
            "std_dev": 1.5,
            "source_temps": {"NWS": 70.0, "VC": 72.0},
            "sum_check": 1.0,
        }

        model = MagicMock()
        model.version = "tier1_equal_weight_v1"
        model.predict.return_value = (Decimal("71.00"), Decimal("1.50"))

        config = _config()
        session = MagicMock()
        stats = run_prediction_cycle(
            config,
            _mock_session_factory(session),
            model=model,
        )

        assert stats["groups_seen"] == 1
        assert stats["predictions_upserted"] == 1
        assert stats["groups_skipped"] == 0
        assert stats["groups_errored"] == 0
        model.predict.assert_called_once()

    @patch(f"{_QUERY_MODULE}.build_probability_distribution")
    @patch(f"{_QUERY_MODULE}.get_or_create_prediction")
    @patch(f"{_QUERY_MODULE}.fetch_markets_by_ids")
    @patch(f"{_QUERY_MODULE}.fetch_all_source_temperatures")
    @patch(f"{_QUERY_MODULE}.fetch_active_market_groups")
    def test_multiple_groups(
        self,
        mock_groups,
        mock_temps,
        mock_markets,
        mock_upsert,
        mock_dist,
    ) -> None:
        """Two groups should both be processed."""
        groups = [
            PredictionGroup(
                city_id=_CITY,
                forecast_date=_DATE,
                market_type=MarketType.HIGH,
                market_ids=(uuid.uuid4(),),
            ),
            PredictionGroup(
                city_id=_CITY,
                forecast_date=_DATE,
                market_type=MarketType.LOW,
                market_ids=(uuid.uuid4(),),
            ),
        ]
        mock_groups.return_value = groups
        _temps = {"NWS": Decimal("70.00"), "VC": Decimal("72.00")}
        mock_temps.return_value = {
            (_CITY, _DATE, MarketType.HIGH): _temps,
            (_CITY, _DATE, MarketType.LOW): _temps,
        }
        mock_markets.return_value = [
            make_market(
                bracket_low=Decimal("65.0000"),
                bracket_high=Decimal("70.0000"),
            )
        ]
        mock_upsert.return_value = (make_prediction(), True)
        mock_dist.return_value = {
            "brackets": {},
            "mean": 71.0,
            "std_dev": 1.5,
            "source_temps": {},
            "sum_check": 0.0,
        }

        model = MagicMock()
        model.version = "tier1_equal_weight_v1"
        model.predict.return_value = (Decimal("71.00"), Decimal("1.50"))

        stats = run_prediction_cycle(
            _config(),
            _mock_session_factory(MagicMock()),
            model=model,
        )

        assert stats["groups_seen"] == 2
        assert stats["predictions_upserted"] == 2


# ---------------------------------------------------------------------------
# Skipping
# ---------------------------------------------------------------------------


class TestPredictionCycleSkipping:
    @patch(f"{_QUERY_MODULE}.fetch_all_source_temperatures")
    @patch(f"{_QUERY_MODULE}.fetch_active_market_groups")
    def test_insufficient_sources_skips(
        self, mock_groups, mock_temps
    ) -> None:
        """Group with fewer than min_sources_required is skipped."""
        group = PredictionGroup(
            city_id=_CITY,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
            market_ids=(uuid.uuid4(),),
        )
        mock_groups.return_value = [group]
        mock_temps.return_value = {
            (_CITY, _DATE, MarketType.HIGH): {"NWS": Decimal("70.00")},
        }

        config = _config(min_sources_required=2)
        stats = run_prediction_cycle(
            config,
            _mock_session_factory(MagicMock()),
        )

        assert stats["groups_seen"] == 1
        assert stats["groups_skipped"] == 1
        assert stats["predictions_upserted"] == 0

    @patch(f"{_QUERY_MODULE}.fetch_active_market_groups")
    def test_no_groups(self, mock_groups) -> None:
        mock_groups.return_value = []

        stats = run_prediction_cycle(
            _config(),
            _mock_session_factory(MagicMock()),
        )

        assert stats["groups_seen"] == 0
        assert stats["predictions_upserted"] == 0

    @patch(f"{_QUERY_MODULE}.fetch_all_source_temperatures")
    @patch(f"{_QUERY_MODULE}.fetch_active_market_groups")
    def test_zero_sources_skips(self, mock_groups, mock_temps) -> None:
        group = PredictionGroup(
            city_id=_CITY,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
            market_ids=(uuid.uuid4(),),
        )
        mock_groups.return_value = [group]
        mock_temps.return_value = {
            (_CITY, _DATE, MarketType.HIGH): {},
        }

        stats = run_prediction_cycle(
            _config(min_sources_required=1),
            _mock_session_factory(MagicMock()),
        )

        assert stats["groups_skipped"] == 1


# ---------------------------------------------------------------------------
# Error isolation
# ---------------------------------------------------------------------------


class TestPredictionCycleErrorIsolation:
    @patch(f"{_QUERY_MODULE}.build_probability_distribution")
    @patch(f"{_QUERY_MODULE}.get_or_create_prediction")
    @patch(f"{_QUERY_MODULE}.fetch_markets_by_ids")
    @patch(f"{_QUERY_MODULE}.fetch_all_source_temperatures")
    @patch(f"{_QUERY_MODULE}.fetch_active_market_groups")
    def test_prediction_error_isolated(
        self,
        mock_groups,
        mock_temps,
        mock_markets,
        mock_upsert,
        mock_dist,
    ) -> None:
        """A PredictionError in one group should not block the next."""
        groups = [
            PredictionGroup(
                city_id=_CITY,
                forecast_date=_DATE,
                market_type=MarketType.HIGH,
                market_ids=(uuid.uuid4(),),
            ),
            PredictionGroup(
                city_id=_CITY,
                forecast_date=_DATE,
                market_type=MarketType.LOW,
                market_ids=(uuid.uuid4(),),
            ),
        ]
        mock_groups.return_value = groups

        # First group: enough sources but model raises
        # Second group: succeeds
        _temps = {"NWS": Decimal("70.00"), "VC": Decimal("72.00")}
        mock_temps.return_value = {
            (_CITY, _DATE, MarketType.HIGH): _temps,
            (_CITY, _DATE, MarketType.LOW): _temps,
        }
        mock_markets.return_value = [
            make_market(
                bracket_low=Decimal("65.0000"),
                bracket_high=Decimal("70.0000"),
            )
        ]
        mock_upsert.return_value = (make_prediction(), True)
        mock_dist.return_value = {
            "brackets": {},
            "mean": 71.0,
            "std_dev": 1.5,
            "source_temps": {},
            "sum_check": 0.0,
        }

        model = MagicMock()
        model.version = "tier1_equal_weight_v1"
        model.predict.side_effect = [
            PredictionError("test failure", source="test"),
            (Decimal("71.00"), Decimal("1.50")),
        ]

        stats = run_prediction_cycle(
            _config(),
            _mock_session_factory(MagicMock()),
            model=model,
        )

        assert stats["groups_seen"] == 2
        assert stats["groups_errored"] == 1
        assert stats["predictions_upserted"] == 1


# ---------------------------------------------------------------------------
# Model selection
# ---------------------------------------------------------------------------


class TestPredictionCycleModelSelection:
    @patch(f"{_QUERY_MODULE}.select_model")
    @patch(f"{_QUERY_MODULE}.fetch_active_market_groups")
    def test_uses_injected_model(
        self, mock_groups, mock_select
    ) -> None:
        """When model= is passed, select_model should NOT be called."""
        mock_groups.return_value = []
        injected_model = MagicMock()
        injected_model.version = "custom_v1"

        run_prediction_cycle(
            _config(),
            _mock_session_factory(MagicMock()),
            model=injected_model,
        )

        mock_select.assert_not_called()

    @patch(f"{_QUERY_MODULE}.select_model")
    @patch(f"{_QUERY_MODULE}.fetch_active_market_groups")
    def test_default_model_selection(
        self, mock_groups, mock_select
    ) -> None:
        """When model= is None, select_model is called."""
        mock_groups.return_value = []
        default_model = MagicMock()
        default_model.version = "tier1_equal_weight_v1"
        mock_select.return_value = default_model

        run_prediction_cycle(
            _config(),
            _mock_session_factory(MagicMock()),
        )

        mock_select.assert_called_once()


# ---------------------------------------------------------------------------
# Prediction upsert
# ---------------------------------------------------------------------------


class TestPredictionUpsert:
    @patch(f"{_QUERY_MODULE}.build_probability_distribution")
    @patch(f"{_QUERY_MODULE}.get_or_create_prediction")
    @patch(f"{_QUERY_MODULE}.fetch_markets_by_ids")
    @patch(f"{_QUERY_MODULE}.fetch_all_source_temperatures")
    @patch(f"{_QUERY_MODULE}.fetch_active_market_groups")
    def test_sets_predicted_temp_and_std(
        self,
        mock_groups,
        mock_temps,
        mock_markets,
        mock_upsert,
        mock_dist,
    ) -> None:
        """Prediction row should be updated with mean and std from dist."""
        group = PredictionGroup(
            city_id=_CITY,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
            market_ids=(uuid.uuid4(),),
        )
        mock_groups.return_value = [group]
        mock_temps.return_value = {
            (_CITY, _DATE, MarketType.HIGH): {
                "NWS": Decimal("68.00"),
                "VC": Decimal("72.00"),
            },
        }
        mock_markets.return_value = [
            make_market(
                bracket_low=Decimal("65.0000"),
                bracket_high=Decimal("70.0000"),
            )
        ]

        prediction = make_prediction()
        mock_upsert.return_value = (prediction, True)

        mock_dist.return_value = {
            "brackets": {"[65.0000, 70.0000)": 0.45},
            "mean": 70.0,
            "std_dev": 2.83,
            "source_temps": {"NWS": 68.0, "VC": 72.0},
            "sum_check": 1.0,
        }

        model = MagicMock()
        model.version = "tier1_equal_weight_v1"
        model.predict.return_value = (Decimal("70.00"), Decimal("2.83"))

        run_prediction_cycle(
            _config(),
            _mock_session_factory(MagicMock()),
            model=model,
        )

        # predicted_temp and std_dev should be set from distribution
        assert prediction.predicted_temp == Decimal("70.00")
        assert prediction.std_dev == Decimal("2.83")
        assert prediction.probability_distribution == mock_dist.return_value

    @patch(f"{_QUERY_MODULE}.build_probability_distribution")
    @patch(f"{_QUERY_MODULE}.get_or_create_prediction")
    @patch(f"{_QUERY_MODULE}.fetch_markets_by_ids")
    @patch(f"{_QUERY_MODULE}.fetch_all_source_temperatures")
    @patch(f"{_QUERY_MODULE}.fetch_active_market_groups")
    def test_reused_prediction_counts_as_upserted(
        self,
        mock_groups,
        mock_temps,
        mock_markets,
        mock_upsert,
        mock_dist,
    ) -> None:
        """get_or_create returning existing still counts as upserted."""
        group = PredictionGroup(
            city_id=_CITY,
            forecast_date=_DATE,
            market_type=MarketType.HIGH,
            market_ids=(uuid.uuid4(),),
        )
        mock_groups.return_value = [group]
        mock_temps.return_value = {
            (_CITY, _DATE, MarketType.HIGH): {
                "NWS": Decimal("70.00"),
                "VC": Decimal("72.00"),
            },
        }
        mock_markets.return_value = [
            make_market(
                bracket_low=Decimal("65.0000"),
                bracket_high=Decimal("70.0000"),
            )
        ]

        prediction = make_prediction()
        mock_upsert.return_value = (prediction, False)  # existing

        mock_dist.return_value = {
            "brackets": {},
            "mean": 71.0,
            "std_dev": 1.5,
            "source_temps": {},
            "sum_check": 0.0,
        }

        model = MagicMock()
        model.version = "tier1_equal_weight_v1"
        model.predict.return_value = (Decimal("71.00"), Decimal("1.50"))

        stats = run_prediction_cycle(
            _config(),
            _mock_session_factory(MagicMock()),
            model=model,
        )

        assert stats["predictions_upserted"] == 1
