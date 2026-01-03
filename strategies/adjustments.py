"""Adjustment request tracking shared by GRVT/PARA.

This module was extracted from the legacy `grvt_adjustments.py` name.

Guideline:
- For any Paradex/PARA API-related fields carried in acknowledgements, reference
  the canonical schemas in `paradex-py/`.
"""

from __future__ import annotations

# Re-export the implementation from the legacy module for now.
# This keeps diffs small while giving us a neutral import path going forward.
from .grvt_adjustments import (  # noqa: F401
    AdjustmentAction,
    AdjustmentAgentState,
    AdjustmentAgentStatus,
    AdjustmentRequest,
    GrvtAdjustmentManager,
)
