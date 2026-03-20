"""Abstract base class for prediction models.

All prediction model tiers implement this interface:
- Tier 1: EqualWeight (available immediately)
- Tier 2: PerformanceWeighted (after 30+ days of settlement data)
- Tier 3: EMOS (after 60+ days — future)

Models are pure math: they receive pre-extracted temperatures and return
(predicted_temp, std_dev).  The caller handles DB reads, temp field
extraction (HIGH vs LOW), and bracket probability mapping.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal

from src.config import PredictionConfig


class PredictionModel(ABC):
    """Interface for all prediction model tiers."""

    @abstractmethod
    def predict(
        self,
        temps: list[Decimal],
        config: PredictionConfig,
    ) -> tuple[Decimal, Decimal]:
        """Produce a point forecast from source temperatures.

        Args:
            temps: Temperature readings from weather sources (one per source).
                Must contain at least ``config.min_sources_required`` values.
            config: Prediction configuration (std_dev_floor, min_sources, …).

        Returns:
            A ``(predicted_temp, std_dev)`` tuple.  ``std_dev`` is guaranteed
            to be >= ``config.std_dev_floor``.

        Raises:
            PredictionError: If ``temps`` has fewer than
                ``config.min_sources_required`` values, or contains
                non-finite Decimals.
        """

    @property
    @abstractmethod
    def version(self) -> str:
        """Model version identifier stored in the Prediction audit trail.

        Must be a stable, unique string per model implementation
        (e.g. ``"tier1_equal_weight_v1"``).
        """
