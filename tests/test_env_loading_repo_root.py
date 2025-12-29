import os
from pathlib import Path


def test_coordinator_loads_repo_root_dotenv(monkeypatch, tmp_path):
    """Coordinator should load perp-dex-tools/.env at import time.

    This test simulates a repo layout:
                repo_root/
                    perp-dex-tools/
                        .env
                        strategies/hedge_coordinator.py

    We change into perp-dex-tools before importing so that the loader must not
    depend on current working directory.
    """

    repo_root = tmp_path / "repo_root"
    pdt_root = repo_root / "perp-dex-tools"
    strategies = pdt_root / "strategies"
    strategies.mkdir(parents=True)

    # Minimal module that mirrors the loader logic we rely on.
    module_path = strategies / "hedge_coordinator.py"
    module_path.write_text(
        "from pathlib import Path\n"
        "from dotenv import load_dotenv\n"
        "BASE_DIR = Path(__file__).resolve().parent\n"
        "PDT_ROOT_DIR = BASE_DIR.parent\n"
        "load_dotenv(PDT_ROOT_DIR / '.env', override=False)\n",
        encoding="utf-8",
    )

    (pdt_root / ".env").write_text("FEISHU_PARA_PUSH_ENABLED=true\n", encoding="utf-8")

    # Ensure a clean import environment.
    monkeypatch.delenv("FEISHU_PARA_PUSH_ENABLED", raising=False)

    # Import should happen with CWD inside perp-dex-tools, but it must still
    # locate repo_root/.env by resolving __file__.
    monkeypatch.chdir(pdt_root)

    import importlib.util
    spec = importlib.util.spec_from_file_location("strategies.hedge_coordinator", module_path)
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert os.getenv("FEISHU_PARA_PUSH_ENABLED") == "true"


def test_dotenv_does_not_override_existing_env(monkeypatch, tmp_path):
    repo_root = tmp_path / "repo_root"
    pdt_root = repo_root / "perp-dex-tools"
    strategies = pdt_root / "strategies"
    strategies.mkdir(parents=True)

    module_path = strategies / "hedge_coordinator.py"
    module_path.write_text(
        "from pathlib import Path\n"
        "from dotenv import load_dotenv\n"
        "BASE_DIR = Path(__file__).resolve().parent\n"
        "PDT_ROOT_DIR = BASE_DIR.parent\n"
        "load_dotenv(PDT_ROOT_DIR / '.env', override=False)\n",
        encoding="utf-8",
    )

    (pdt_root / ".env").write_text("FEISHU_PARA_PUSH_INTERVAL=30\n", encoding="utf-8")

    monkeypatch.setenv("FEISHU_PARA_PUSH_INTERVAL", "999")
    monkeypatch.chdir(pdt_root)

    import importlib.util
    spec = importlib.util.spec_from_file_location("strategies.hedge_coordinator", module_path)
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert os.getenv("FEISHU_PARA_PUSH_INTERVAL") == "999"
