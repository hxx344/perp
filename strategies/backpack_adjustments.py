"""Backpack adjustment queue for monitor-executed trades.

This mirrors the para/grvt adjustment flow:
- Coordinator stores pending requests.
- Monitor polls /control and receives pending adjustments.
- Monitor executes trade (market/TWAP) and POSTs /backpack/adjust/ack.

Request payload shape is intentionally aligned with existing adjustments:
{
  "request_id": "uuid",
  "agent_id": "backpack-agent",
  "action": "add"|"reduce",
  "magnitude": 1.23,
  "symbols": ["SOL_USDC"],
  "payload": {"order_mode": "twap", "twap_duration_seconds": 60, "algo_type": "TWAP"},
  "provider": "backpack"
}

The coordinator only routes; monitor is the executor.
"""

from __future__ import annotations

import copy
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class BackpackAdjustment:
    request_id: str
    agent_id: str
    action: str
    magnitude: Any
    symbols: List[str]
    payload: Dict[str, Any]
    created_at: float
    status: str = "pending"  # pending|succeeded|failed
    note: Optional[str] = None
    acked_at: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "request_id": self.request_id,
            "agent_id": self.agent_id,
            "action": self.action,
            "magnitude": self.magnitude,
            "symbols": list(self.symbols),
            "payload": copy.deepcopy(self.payload),
            "created_at": self.created_at,
            "status": self.status,
            "provider": "backpack",
        }
        if self.note is not None:
            data["note"] = self.note
        if self.acked_at is not None:
            data["acked_at"] = self.acked_at
        return data


class BackpackAdjustmentManager:
    def __init__(self) -> None:
        self._pending: Dict[str, BackpackAdjustment] = {}

    def create(
        self,
        *,
        agent_id: str,
        action: str,
        magnitude: Any,
        symbols: List[str],
        payload: Optional[Dict[str, Any]] = None,
    ) -> BackpackAdjustment:
        req_id = str(uuid.uuid4())
        adj = BackpackAdjustment(
            request_id=req_id,
            agent_id=str(agent_id),
            action=str(action),
            magnitude=magnitude,
            symbols=list(symbols),
            payload=dict(payload or {}),
            created_at=time.time(),
        )
        self._pending[req_id] = adj
        return adj

    async def pending_for_agent(self, agent_id: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for adj in self._pending.values():
            if adj.agent_id == agent_id and adj.status == "pending":
                out.append(adj.to_dict())
        return out

    async def list_all(self) -> List[Dict[str, Any]]:
        return [adj.to_dict() for adj in self._pending.values()]

    async def acknowledge(self, *, request_id: str, agent_id: str, status: str, note: Optional[str], extra: Optional[Dict[str, Any]] = None) -> bool:
        adj = self._pending.get(request_id)
        if adj is None:
            return False
        if adj.agent_id != agent_id:
            return False
        adj.status = str(status)
        if note is not None:
            adj.note = str(note)
        adj.acked_at = time.time()
        # Store any extra fields for visibility.
        if isinstance(extra, dict) and extra:
            merged = dict(adj.payload)
            merged.update(extra)
            adj.payload = merged
        return True
