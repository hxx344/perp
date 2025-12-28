import asyncio
import sys
from pathlib import Path

# Ensure repo root is on PYTHONPATH when running as a script.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from strategies.hedge_coordinator import HedgeCoordinator, RiskAlertSettings


async def main() -> None:
    settings = RiskAlertSettings(
        threshold=0.3,
        reset_ratio=0.2,
        cooldown=0,
        bark_url="http://example.com/{title}/{body}",
        bark_timeout=1,
        bark_append_payload=False,
        title_template="",
        body_template="",
    )
    coordinator = HedgeCoordinator(alert_settings=settings)

    before = await coordinator.alert_history_snapshot()
    await coordinator.trigger_test_alert({"ratio": 0.31})
    after = await coordinator.alert_history_snapshot()

    print(f"before={len(before)} after={len(after)}")
    if after:
        latest = after[0]
        print(
            "latest:",
            {
                "kind": latest.get("kind"),
                "source": latest.get("source"),
                "status": latest.get("status"),
                "ratio": latest.get("ratio"),
                "account_label": latest.get("account_label"),
            },
        )


if __name__ == "__main__":
    asyncio.run(main())
