"""Runtime behavior tests for prediction-engine internals."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock
from datetime import UTC, datetime

from sqlalchemy.exc import IntegrityError


def _load_module(module_name: str, module_path: Path) -> ModuleType:
    service_root = module_path.parents[1]
    sys.path.insert(0, str(service_root))
    try:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        return module
    finally:
        sys.path.pop(0)


def _load_prediction_main_module() -> ModuleType:
    service_root = Path(__file__).resolve().parents[1]
    return _load_module("prediction_engine_main_runtime_test_module", service_root / "src" / "main.py")


def _load_migration_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    return _load_module(
        "prediction_engine_uniques_migration_test_module",
        repo_root
        / "shared"
        / "db-migrations"
        / "versions"
        / "2026_03_18_k3l8m1n2o5p6_add_prediction_recommendation_uniques.py",
    )


class _FakeResult:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def scalars(self) -> "_FakeResult":
        return self

    def all(self) -> list[object]:
        return self._rows


def test_get_or_create_prediction_recovers_from_unique_race() -> None:
    from src.data.queries import get_or_create_prediction
    from shared.db.enums import MarketType

    existing_prediction = SimpleNamespace(id="prediction-1")
    session = MagicMock()

    # First execute: nothing found
    first_result = MagicMock()
    first_result.scalars.return_value.first.return_value = None
    # Second execute (after IntegrityError): existing found
    second_result = MagicMock()
    second_result.scalar_one.return_value = existing_prediction

    session.execute.side_effect = [first_result, second_result]
    session.flush.side_effect = IntegrityError("stmt", "params", Exception("unique violation"))
    session.begin_nested.return_value.__enter__ = MagicMock(return_value=None)
    session.begin_nested.return_value.__exit__ = MagicMock(return_value=False)

    prediction, created = get_or_create_prediction(
        session,
        city_id="city-1",
        forecast_date=datetime(2026, 3, 19, tzinfo=UTC),
        market_type=MarketType.HIGH,
        model_version="v1",
    )

    assert prediction is existing_prediction
    assert created is False


def test_latest_snapshot_query_selects_one_row_per_market() -> None:
    from src.data.queries import fetch_latest_snapshot_map

    latest_a = SimpleNamespace(market_id="market-a", timestamp=2)
    latest_b = SimpleNamespace(market_id="market-b", timestamp=3)
    executed_sql: list[str] = []

    class _Session:
        def execute(self, stmt):
            sql = str(stmt).lower()
            executed_sql.append(sql)
            return _FakeResult([latest_a, latest_b])

    snapshots = fetch_latest_snapshot_map(_Session())

    assert snapshots == {
        "market-a": latest_a,
        "market-b": latest_b,
    }
    assert executed_sql
    assert "row_number()" in executed_sql[0]
    assert "partition by" in executed_sql[0]
    assert "row_num = 1" in executed_sql[0] or "row_num = :row_num_1" in executed_sql[0]


def test_uniqueness_migration_updates_children_before_deleting_duplicates() -> None:
    module = _load_migration_module()
    recorded_executes: list[str] = []
    created_constraints: list[tuple[str, str, tuple[str, ...]]] = []

    class _Op:
        def execute(self, sql: str) -> None:
            recorded_executes.append(sql.strip())

        def create_unique_constraint(self, name: str, table: str, columns: list[str]) -> None:
            created_constraints.append((name, table, tuple(columns)))

        def drop_constraint(self, *args, **kwargs) -> None:
            raise AssertionError("downgrade() is not part of this test")

    module.op = _Op()
    module.upgrade()

    assert len(recorded_executes) == 8
    assert "update recommendations" in recorded_executes[0].lower()
    assert "delete from predictions" in recorded_executes[1].lower()
    assert "delete from paper_trades_fixed" in recorded_executes[2].lower()
    assert "update paper_trades_fixed" in recorded_executes[3].lower()
    assert "update paper_trades_portfolio" in recorded_executes[4].lower()
    assert "delete from email_log_recommendations" in recorded_executes[5].lower()
    assert "update email_log_recommendations" in recorded_executes[6].lower()
    assert "delete from recommendations" in recorded_executes[7].lower()
    assert created_constraints == [
        ("uq_prediction_city_date_type_model", "predictions", ("city_id", "forecast_date", "market_type", "model_version")),
        ("uq_recommendation_prediction_market", "recommendations", ("prediction_id", "market_id")),
    ]
