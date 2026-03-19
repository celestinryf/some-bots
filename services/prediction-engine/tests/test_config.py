"""
Tests for PredictionConfig validation and loading.
"""

from decimal import Decimal

import pytest

from src.config import PredictionConfig, _env_decimal, _env_int, load_prediction_config


class TestPredictionConfig:
    def test_defaults_are_valid(self) -> None:
        config = PredictionConfig()
        assert config.gap_threshold == Decimal("0.20")
        assert config.min_ev_threshold == Decimal("0.08")
        assert config.min_sources_required == 2
        assert config.std_dev_floor == Decimal("1.50")

    def test_risk_weights_sum_to_one(self) -> None:
        config = PredictionConfig()
        weight_sum = sum(config.risk_weights.values())
        assert weight_sum == Decimal("1.00")

    def test_risk_weights_property(self) -> None:
        config = PredictionConfig()
        weights = config.risk_weights
        assert len(weights) == 6
        assert "forecast_spread" in weights
        assert "source_agreement" in weights
        assert "city_accuracy" in weights
        assert "liquidity" in weights
        assert "bracket_edge" in weights
        assert "lead_time" in weights

    def test_invalid_risk_weights_not_summing_to_one(self) -> None:
        with pytest.raises(ValueError, match="sum to 1.0"):
            PredictionConfig(risk_weight_forecast_spread=Decimal("0.50"))

    def test_invalid_gap_threshold_zero(self) -> None:
        with pytest.raises(ValueError, match="gap_threshold must be in"):
            PredictionConfig(gap_threshold=Decimal("0"))

    def test_invalid_gap_threshold_negative(self) -> None:
        with pytest.raises(ValueError, match="gap_threshold must be in"):
            PredictionConfig(gap_threshold=Decimal("-0.10"))

    def test_invalid_gap_threshold_above_one(self) -> None:
        with pytest.raises(ValueError, match="gap_threshold must be in"):
            PredictionConfig(gap_threshold=Decimal("1.50"))

    def test_invalid_min_ev_threshold_zero(self) -> None:
        with pytest.raises(ValueError, match="min_ev_threshold must be in"):
            PredictionConfig(min_ev_threshold=Decimal("0"))

    def test_invalid_min_ev_threshold_above_one(self) -> None:
        with pytest.raises(ValueError, match="min_ev_threshold must be in"):
            PredictionConfig(min_ev_threshold=Decimal("1.50"))

    def test_invalid_min_sources_zero(self) -> None:
        with pytest.raises(ValueError, match="min_sources_required must be >= 1"):
            PredictionConfig(min_sources_required=0)

    def test_invalid_std_dev_floor_zero(self) -> None:
        with pytest.raises(ValueError, match="std_dev_floor must be positive"):
            PredictionConfig(std_dev_floor=Decimal("0"))

    def test_invalid_probability_sum_tolerance_zero(self) -> None:
        with pytest.raises(ValueError, match="probability_sum_tolerance must be positive"):
            PredictionConfig(probability_sum_tolerance=Decimal("0"))

    def test_frozen(self) -> None:
        config = PredictionConfig()
        with pytest.raises(AttributeError):
            config.gap_threshold = Decimal("0.15")  # type: ignore[misc]

    def test_custom_values(self) -> None:
        config = PredictionConfig(
            gap_threshold=Decimal("0.15"),
            min_ev_threshold=Decimal("0.05"),
            std_dev_floor=Decimal("2.00"),
        )
        assert config.gap_threshold == Decimal("0.15")
        assert config.min_ev_threshold == Decimal("0.05")
        assert config.std_dev_floor == Decimal("2.00")

    def test_invalid_min_sources_negative(self) -> None:
        with pytest.raises(ValueError, match="min_sources_required must be >= 1"):
            PredictionConfig(min_sources_required=-1)


# ---------------------------------------------------------------------------
# _env_decimal / _env_int helpers
# ---------------------------------------------------------------------------


class TestEnvHelpers:
    def test_env_decimal_returns_default_when_unset(self) -> None:
        result = _env_decimal("NONEXISTENT_VAR_XYZ", Decimal("0.42"))
        assert result == Decimal("0.42")

    def test_env_decimal_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_PREDICTION_VAL", "0.30")
        result = _env_decimal("TEST_PREDICTION_VAL", Decimal("0.20"))
        assert result == Decimal("0.30")

    def test_env_decimal_invalid_value_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_PREDICTION_VAL", "not_a_number")
        with pytest.raises(ValueError, match="valid decimal"):
            _env_decimal("TEST_PREDICTION_VAL", Decimal("0.20"))

    def test_env_int_returns_default_when_unset(self) -> None:
        result = _env_int("NONEXISTENT_VAR_XYZ", 42)
        assert result == 42

    def test_env_int_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_PREDICTION_INT", "5")
        result = _env_int("TEST_PREDICTION_INT", 2)
        assert result == 5

    def test_env_int_invalid_value_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_PREDICTION_INT", "abc")
        with pytest.raises(ValueError, match="integer"):
            _env_int("TEST_PREDICTION_INT", 2)


# ---------------------------------------------------------------------------
# load_prediction_config
# ---------------------------------------------------------------------------


class TestLoadPredictionConfig:
    def test_defaults_match_dataclass(self) -> None:
        config = load_prediction_config()
        defaults = PredictionConfig()
        assert config.gap_threshold == defaults.gap_threshold
        assert config.min_ev_threshold == defaults.min_ev_threshold
        assert config.model_version == defaults.model_version
        assert config.min_sources_required == defaults.min_sources_required

    def test_env_override_gap_threshold(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREDICTION_GAP_THRESHOLD", "0.15")
        config = load_prediction_config()
        assert config.gap_threshold == Decimal("0.15")

    def test_env_override_model_version(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREDICTION_MODEL_VERSION", "tier2_perf_weighted_v1")
        config = load_prediction_config()
        assert config.model_version == "tier2_perf_weighted_v1"

    def test_env_override_min_sources(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREDICTION_MIN_SOURCES_REQUIRED", "3")
        config = load_prediction_config()
        assert config.min_sources_required == 3

    def test_invalid_env_value_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREDICTION_GAP_THRESHOLD", "not_a_decimal")
        with pytest.raises(ValueError, match="valid decimal"):
            load_prediction_config()
