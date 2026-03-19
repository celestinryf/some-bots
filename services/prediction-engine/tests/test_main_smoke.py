"""Startup smoke tests for prediction-engine entrypoints."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace


def _load_prediction_main_module() -> ModuleType:
    service_root = Path(__file__).resolve().parents[1]
    module_path = service_root / "src" / "main.py"
    previous_src_modules = {
        name: module for name, module in sys.modules.items() if name == "src" or name.startswith("src.")
    }

    for name in list(sys.modules):
        if name == "src" or name.startswith("src."):
            sys.modules.pop(name, None)

    sys.path.insert(0, str(service_root))
    try:
        spec = importlib.util.spec_from_file_location(
            "prediction_engine_main_test_module",
            module_path,
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        module_name = module.__name__
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        return module
    finally:
        sys.path.pop(0)
        for name in list(sys.modules):
            if name == "src" or name.startswith("src."):
                sys.modules.pop(name, None)
        sys.modules.update(previous_src_modules)


def test_prediction_entrypoint_smoke(monkeypatch) -> None:
    module = _load_prediction_main_module()
    captured: dict[str, object] = {}

    monkeypatch.setattr(module, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        module,
        "get_settings",
        lambda: SimpleNamespace(log_level="INFO", environment="test"),
    )
    monkeypatch.setattr(module, "setup_logging", lambda _level: None)
    monkeypatch.setattr(module, "load_prediction_config", lambda: module.PredictionConfig())

    def fake_run_service_loop(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(module, "_run_service_loop", fake_run_service_loop)

    exit_code = module.main(["--role", "prediction", "--run-once"])

    assert exit_code == 0
    assert captured["role"] == module.ServiceRole.PREDICTION
    assert captured["run_once"] is True


def test_recommendation_entrypoint_smoke(monkeypatch) -> None:
    module = _load_prediction_main_module()
    captured: dict[str, object] = {}

    monkeypatch.setattr(module, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        module,
        "get_settings",
        lambda: SimpleNamespace(log_level="INFO", environment="test"),
    )
    monkeypatch.setattr(module, "setup_logging", lambda _level: None)
    monkeypatch.setattr(module, "load_prediction_config", lambda: module.PredictionConfig())

    def fake_run_service_loop(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(module, "_run_service_loop", fake_run_service_loop)

    exit_code = module.main(["--role", "recommendation", "--run-once"])

    assert exit_code == 0
    assert captured["role"] == module.ServiceRole.RECOMMENDATION
    assert captured["run_once"] is True
