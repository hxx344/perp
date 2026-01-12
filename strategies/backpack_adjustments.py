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
from dataclasses import dataclass, field
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
    # Per-agent acknowledgement details (useful when agent_id == "all").
    acks: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        # Compute an aggregate status when there are per-agent ACKs.
        overall_status: Optional[str] = None
        if self.acks:
            statuses = [str(v.get("status") or "").lower() for v in self.acks.values()]
            if any(s in {"failed", "error"} for s in statuses):
                overall_status = "failed"
            elif statuses and all(s in {"succeeded", "success", "completed"} for s in statuses):
                overall_status = "completed"
            else:
                overall_status = "in_progress"

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
        if self.acks:
            data["agents"] = [
                {"agent_id": agent_id, **copy.deepcopy(info)}
                for agent_id, info in sorted(self.acks.items(), key=lambda kv: kv[0])
            ]
        if overall_status is not None:
            data["overall_status"] = overall_status
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
            # Broadcast requests are stored with agent_id == "all".
            # Any agent may pick them up.
            if adj.status != "pending":
                continue
            if adj.agent_id == agent_id or adj.agent_id == "all":
                out.append(adj.to_dict())
        return out

    async def list_all(self) -> List[Dict[str, Any]]:
        return [adj.to_dict() for adj in self._pending.values()]

    async def acknowledge(self, *, request_id: str, agent_id: str, status: str, note: Optional[str], extra: Optional[Dict[str, Any]] = None) -> bool:
        adj = self._pending.get(request_id)
        if adj is None:
            return False
        # For broadcast requests (agent_id == "all"), allow any agent to ACK.
        # For targeted requests, enforce agent identity.
        if adj.agent_id != "all" and adj.agent_id != agent_id:
            return False

        st = str(status)
        now = time.time()
        if adj.agent_id == "all":
            # Track per-agent ACKs.
            ack_info: Dict[str, Any] = {
                "status": st,
                "acked_at": now,
            }
            if note is not None:
                ack_info["note"] = str(note)
            if isinstance(extra, dict) and extra:
                ack_info["extra"] = copy.deepcopy(extra)
            adj.acks[str(agent_id)] = ack_info
            adj.acked_at = now
            # Keep legacy status as pending until at least one ACK arrives.
            # Once any ACK arrives, surface as in_progress via overall_status.
        else:
            # Targeted request legacy behavior.
            adj.status = st
            if note is not None:
                adj.note = str(note)
            adj.acked_at = now
            # Store any extra fields for visibility.
            if isinstance(extra, dict) and extra:
                merged = dict(adj.payload)
                merged.update(extra)
                adj.payload = merged
        return True
