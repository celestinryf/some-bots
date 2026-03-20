"""Model selector: picks the best available prediction tier.

Tier selection is based on the amount of settlement data available:
- Tier 1 (EqualWeight): always available — cold-start default
- Tier 2 (PerformanceWeighted): after 30+ settled markets (stub)
- Tier 3 (EMOS): after 60+ settled markets (future)

The selector is intentionally simple — it returns a PredictionModel
instance, and the caller doesn't need to know which tier was chosen.
"""

from __future__ import annotations

from src.models.base import PredictionModel
from src.models.equal_weight import EqualWeightModel

# Threshold at which Tier 2 becomes eligible (Decision #1: Strategy Pattern)
_TIER2_MIN_SETTLEMENTS = 30


def select_model(settlement_count: int | None = None) -> PredictionModel:
    """Select the best available prediction model tier.

    Args:
        settlement_count: Number of settled markets with verified outcomes.
            If ``None`` or below threshold, returns Tier 1 (cold start).

    Returns:
        A ``PredictionModel`` instance for the selected tier.
    """
    # Tier 2 stub: uncomment when PerformanceWeightedModel is implemented
    # if settlement_count is not None and settlement_count >= _TIER2_MIN_SETTLEMENTS:
    #     from src.models.performance_weighted import PerformanceWeightedModel
    #     return PerformanceWeightedModel()

    return EqualWeightModel()
