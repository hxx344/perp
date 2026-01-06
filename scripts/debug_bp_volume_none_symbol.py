import asyncio
import sys
from pathlib import Path

# Allow running from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import strategies.hedge_coordinator as hc


async def main() -> None:
    app = hc.CoordinatorApp(enable_volatility_monitor=False)

    async with app._bp_volume_lock:
        app._bp_volume.running = True
        app._bp_volume.run_id = "r1"
        app._bp_volume.cfg.symbol = None  # type: ignore[assignment]
        print("cfg.symbol set to", repr(app._bp_volume.cfg.symbol))

    task = asyncio.create_task(app._bp_volume_runner("r1"))
    await asyncio.sleep(0.3)

    async with app._bp_volume_lock:
        print("running", app._bp_volume.running)
        print("last_error", app._bp_volume.last_error)

    print("task.done", task.done())
    if task.done():
        try:
            exc = task.exception()
            print("task.exception", type(exc).__name__ if exc else None, str(exc) if exc else "")
        except BaseException as exc:
            print("task.exception raised", type(exc).__name__, str(exc))

        try:
            task.result()
        except BaseException as exc:
            import traceback

            print("task.result raised", type(exc).__name__, str(exc))
            traceback.print_exc()

    task.cancel()
    try:
        await task
    except BaseException as exc:
        print("task_stop", type(exc).__name__, str(exc))


if __name__ == "__main__":
    asyncio.run(main())
