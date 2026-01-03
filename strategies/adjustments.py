"""Adjustment request tracking shared by GRVT/PARA.

This module was extracted from the legacy `grvt_adjustments.py` name.

Guideline:
- For any Paradex/PARA API-related fields carried in acknowledgements, reference
  the canonical schemas in `paradex-py/`.
"""

from __future__ import annotations

# Re-export the implementation from the legacy module for now.
# This keeps diffs small while giving us a neutral import path going forward.
#
# IMPORTANT: This module must work in both of these execution modes:
# 1) Package import: `from strategies.adjustments import ...` (relative import works)
# 2) Script-style execution where `strategies/` isn't a package parent (no known parent package)
#    In that case we fall back to absolute imports.
try:
  from .grvt_adjustments import (  # type: ignore  # noqa: F401
    AdjustmentAction,
    AdjustmentAgentState,
    AdjustmentAgentStatus,
    AdjustmentRequest,
    GrvtAdjustmentManager,
  )
except ImportError:  # pragma: no cover
  from grvt_adjustments import (  # type: ignore  # noqa: F401
    AdjustmentAction,
    AdjustmentAgentState,
    AdjustmentAgentStatus,
    AdjustmentRequest,
    GrvtAdjustmentManager,
  )
