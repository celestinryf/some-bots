"""Startup smoke tests for the notification-service entrypoint."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace


def _load_notification_main_module() -> ModuleType:
    service_root = Path(__file__).resolve().parents[1]
    module_path = service_root / "src" / "main.py"
    previous_src_modules = {
        name: module for name, module in sys.modules.items() if name == "src" or name.startswith("src.")
    }

    for name in list(sys.modules):
        if name == "src" or name.startswith("src."):
            sys.modules.pop(name, None)

    spec = importlib.util.spec_from_file_location(
        "notification_service_main",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise AssertionError("Failed to load notification-service main module")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module.__name__] = module
    sys.path.insert(0, str(service_root))
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)
        for name in list(sys.modules):
            if name == "src" or name.startswith("src."):
                sys.modules.pop(name, None)
        sys.modules.update(previous_src_modules)


def test_notification_entrypoint_smoke(monkeypatch) -> None:
    module = _load_notification_main_module()
    captured: dict[str, object] = {}

    monkeypatch.setattr(module, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        module,
        "get_settings",
        lambda: SimpleNamespace(log_level="INFO", environment="test"),
    )
    monkeypatch.setattr(module, "setup_logging", lambda _level: None)

    def fake_run_service_loop(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(module, "_run_service_loop", fake_run_service_loop)

    exit_code = module.main(["--run-once", "--digest-window-hours", "12"])

    assert exit_code == 0
    assert captured["run_once"] is True
    assert captured["digest_window_hours"] == 12
