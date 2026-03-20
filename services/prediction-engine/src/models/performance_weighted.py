"""Tier 2 prediction model: performance-weighted ensemble (stub).

Weights each weather source by its historical accuracy for the target
city and season.  Requires 30+ days of settlement data to calibrate.

This is a placeholder — the full implementation will be built during
Sprint 6 (validation period) once sufficient settlement data has
accumulated.
"""

from __future__ import annotations

from decimal import Decimal

from src.config import PredictionConfig
from src.models.base import PredictionModel

from shared.config.errors import PredictionError


class PerformanceWeightedModel(PredictionModel):
    """Performance-weighted ensemble (not yet implemented).

    Raises PredictionError when called — callers must check
    ``ModelSelector.select()`` which only returns this model when
    sufficient settlement data exists.
    """

    @property
    def version(self) -> str:
        return "tier2_performance_weighted_v1"

    def predict(
        self,
        temps: list[Decimal],
        config: PredictionConfig,
    ) -> tuple[Decimal, Decimal]:
        """Not yet implemented.

        Raises:
            PredictionError: Always — this model requires settlement data
                that hasn't been collected yet.
        """
        raise PredictionError(
            "PerformanceWeightedModel requires 30+ days of settlement data; "
            "not yet implemented",
            source="performance-weighted-model",
        )
