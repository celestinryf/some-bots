"""Tier 1 prediction model: equal-weight ensemble averaging.

Each weather source contributes equally to the predicted temperature.
This is the cold-start model — available immediately with no historical
settlement data required.

Uses Phase 1 pure-math functions from engine.probability:
- compute_ensemble_mean  → equal-weight average
- compute_ensemble_std   → population std with configurable floor
"""

from __future__ import annotations

from decimal import Decimal

from src.config import PredictionConfig
from src.engine.probability import compute_ensemble_mean, compute_ensemble_std
from src.models.base import PredictionModel

from shared.config.errors import PredictionError


class EqualWeightModel(PredictionModel):
    """Equal-weight ensemble of weather source temperatures.

    Computes a simple average of all provided source temperatures and
    returns a standard deviation with a configurable floor to prevent
    overconfident predictions when sources agree exactly.
    """

    @property
    def version(self) -> str:
        return "tier1_equal_weight_v1"

    def predict(
        self,
        temps: list[Decimal],
        config: PredictionConfig,
    ) -> tuple[Decimal, Decimal]:
        """Compute equal-weight mean and std from source temperatures.

        Args:
            temps: One temperature per weather source. Must have at least
                ``config.min_sources_required`` entries.
            config: Prediction configuration.

        Returns:
            ``(predicted_temp, std_dev)`` where std_dev >= config.std_dev_floor.

        Raises:
            PredictionError: If too few sources or non-finite values.
        """
        if len(temps) < config.min_sources_required:
            raise PredictionError(
                f"Need >= {config.min_sources_required} sources, got {len(temps)}",
                source="equal-weight-model",
            )

        mean = compute_ensemble_mean(temps)
        std = compute_ensemble_std(temps, config.std_dev_floor)
        return mean, std
