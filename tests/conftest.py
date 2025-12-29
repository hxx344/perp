"""Pytest configuration for perp-dex-tools.

Many tests import modules as `from strategies...` because `perp-dex-tools/` is
intended to be on `PYTHONPATH` when run as a repo.

When running pytest from other working directories (or IDE runners), the repo
root may not be automatically added to sys.path on Windows.

This conftest ensures `perp-dex-tools/` is importable, so `strategies` can be
resolved as a top-level package.
"""

from __future__ import annotations

import sys
from pathlib import Path


PDT_ROOT = Path(__file__).resolve().parents[1]
if str(PDT_ROOT) not in sys.path:
    sys.path.insert(0, str(PDT_ROOT))
