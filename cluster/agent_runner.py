"""Agent runner that connects to the cluster coordinator and controls the maker."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

import aiohttp

from strategies.lighter_simple_market_maker import (
    SimpleMakerSettings,
    SimpleMarketMaker,
    _parse_args as parse_strategy_args,
)


LOGGER = logging.getLogger("cluster.agent")


def _decimal_from(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


async def _read_json(response: aiohttp.ClientResponse) -> Dict[str, Any]:
    if response.status >= 400:
        try:
            payload = await response.text()
        except Exception:  # pragma: no cover - defensive
            payload = "<unavailable>"
        raise RuntimeError(f"Coordinator request failed: {response.status} {payload}")
    return await response.json()


class AgentRuntime:
    def __init__(
        self,
        settings: SimpleMakerSettings,
        *,
        coordinator_url: str,
        vps_id: str,
        role: Optional[str],
        command_poll_interval: float,
        metrics_interval: float,
        session: aiohttp.ClientSession,
        random_seed: Optional[int],
    ) -> None:
        self.settings = settings
        self.coordinator_url = coordinator_url.rstrip("/")
        self.vps_id = vps_id
        self.role = role
        self.command_poll_interval = max(0.5, command_poll_interval)
        self.metrics_interval = max(1.0, metrics_interval)
        self.session = session
        self.random_seed = random_seed
        self._maker: Optional[SimpleMarketMaker] = None
        self._last_action: Optional[str] = None
        self._last_command_signature: Optional[str] = None

    async def register(self) -> Dict[str, Any]:
        payload = {"vps_id": self.vps_id}
        if self.role:
            payload["role"] = self.role
        async with self.session.post(f"{self.coordinator_url}/register", json=payload) as response:
            data = await _read_json(response)
        LOGGER.info("Registered with coordinator: %s", data)
        return data

    async def run(self) -> None:
        async with SimpleMarketMaker(self.settings) as maker:
            self._maker = maker
            maker.set_random_seed(self.random_seed)
            if self.settings.allowed_sides is not None:
                maker.set_allowed_sides(self.settings.allowed_sides)
            if (
                self.settings.order_quantity_min is not None
                or self.settings.order_quantity_max is not None
            ):
                maker.set_quantity_range(
                    self.settings.order_quantity_min,
                    self.settings.order_quantity_max,
                )
            maker.pause_trading()
            command_task = asyncio.create_task(self._command_loop())
            metrics_task = asyncio.create_task(self._metrics_loop())
            maker_task = asyncio.create_task(maker.run())
            tasks = {command_task, metrics_task, maker_task}
            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
                for task in done:
                    exc = task.exception()
                    if exc:
                        raise exc
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _command_loop(self) -> None:
        assert self._maker is not None
        while True:
            try:
                params = {"vps_id": self.vps_id}
                async with self.session.get(f"{self.coordinator_url}/command", params=params) as response:
                    command = await _read_json(response)
                await self._apply_command(command)
            except Exception as exc:  # pragma: no cover - networking
                LOGGER.error("Command loop error: %s", exc)
            await asyncio.sleep(self.command_poll_interval)

    async def _metrics_loop(self) -> None:
        assert self._maker is not None
        while True:
            try:
                snapshot = self._maker.export_position_snapshot()
                account_metrics = self._maker.export_account_metrics()
                net_position = self._maker.current_net_position()
                payload = {
                    "vps_id": self.vps_id,
                    "net_position": str(net_position),
                    "timestamp": time.time(),
                    "positions": snapshot,
                    "account_metrics": account_metrics,
                }
                async with self.session.post(f"{self.coordinator_url}/metrics", json=payload) as response:
                    await _read_json(response)
            except Exception as exc:  # pragma: no cover - networking
                LOGGER.error("Metrics loop error: %s", exc)
            await asyncio.sleep(self.metrics_interval)

    async def _apply_command(self, command: Dict[str, Any]) -> None:
        assert self._maker is not None
        action = str(command.get("action", "WAIT")).upper()
        if not action or action == "WAIT":
            return

        signature = None
        if action == "RUN":
            try:
                signature = json.dumps(command, sort_keys=True, default=str)
            except TypeError:
                signature = str(command)
            if signature == self._last_command_signature:
                return

        reason = command.get("reason")
        if reason:
            LOGGER.info("Command %s: %s", action, reason)
        else:
            LOGGER.info("Command %s", action)

        if action == "RUN":
            allowed_sides = command.get("allowed_sides")
            if allowed_sides:
                self._maker.set_allowed_sides(allowed_sides)
            else:
                side = command.get("side", "buy")
                self._maker.set_allowed_sides([side])
            quantity_payload = command.get("quantity") or {}
            min_qty = _decimal_from(quantity_payload.get("min"), default=str(self.settings.order_quantity))
            max_qty = _decimal_from(quantity_payload.get("max"), default=str(self.settings.order_quantity))
            self._maker.set_quantity_range(min_qty, max_qty)
            self._maker.resume_trading()
        elif action == "PAUSE":
            self._maker.pause_trading()
        elif action == "FLATTEN":
            metadata = command.get("metadata") if isinstance(command, dict) else None
            tolerance_value = None
            price_offset_ticks = 0
            if isinstance(metadata, dict):
                tolerance_value = metadata.get("tolerance")
                maybe_offset = metadata.get("price_offset_ticks")
                if maybe_offset is not None:
                    try:
                        price_offset_ticks = int(maybe_offset)
                    except (TypeError, ValueError):
                        price_offset_ticks = 0
            tolerance = _decimal_from(tolerance_value, default="0.01") if tolerance_value is not None else None
            await self._maker.emergency_flatten(
                tolerance=tolerance,
                price_offset_ticks=price_offset_ticks,
            )
        else:
            LOGGER.warning("Unknown action '%s'", action)

        self._last_action = action
        self._last_command_signature = signature if action == "RUN" else action


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Agent wrapper for SimpleMarketMaker")
    parser.add_argument("--coordinator-url", required=True, help="Coordinator base URL, e.g. http://127.0.0.1:8080")
    parser.add_argument("--vps-id", required=True, help="Unique identifier for this VPS")
    parser.add_argument("--role", choices=["primary", "hedge"], default=None, help="Preferred role (optional)")
    parser.add_argument("--command-poll-interval", type=float, default=1.0, help="Seconds between command polls")
    parser.add_argument("--metrics-interval", type=float, default=2.0, help="Seconds between metrics uploads")
    parser.add_argument("--random-seed", type=int, default=None, help="Optional RNG seed for reproducible quantity sampling")
    return parser


def main() -> None:
    parser = build_parser()
    agent_args, strategy_arg_list = parser.parse_known_args()

    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")

    strategy_settings = parse_strategy_args(strategy_arg_list)

    async def _async_main() -> None:
        async with aiohttp.ClientSession() as session:
            runtime = AgentRuntime(
                strategy_settings,
                coordinator_url=agent_args.coordinator_url,
                vps_id=agent_args.vps_id,
                role=agent_args.role,
                command_poll_interval=agent_args.command_poll_interval,
                metrics_interval=agent_args.metrics_interval,
                session=session,
                random_seed=agent_args.random_seed,
            )
            await runtime.register()
            await runtime.run()

    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        LOGGER.info("Agent interrupted; shutting down")


if __name__ == "__main__":
    main()
