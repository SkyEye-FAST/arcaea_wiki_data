"""Run the listened update flow and sync wiki version as soon as it changes."""

import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pywikibot
import requests

import update
from sync_wiki import (
    ensure_authenticated,
    ensure_inputs,
    materialize_password_file_from_env,
    sync_pages,
)

UPDATE_SKIPPED_MARKER = update.PROJECT_ROOT / ".update-skipped"
UPDATE_LISTEN_TIMEZONE = ZoneInfo("Asia/Shanghai")
UPDATE_LISTEN_START = (7, 50)
UPDATE_LISTEN_END = (8, 10)
UPDATE_LISTEN_POLL_SECONDS = 10


def main() -> None:
    """Listen for a new APK version, pre-sync wiki version, then run update.py."""
    UPDATE_SKIPPED_MARKER.unlink(missing_ok=True)

    current_version = ""
    if update.OUTPUT_VERSION_FILE.exists():
        current_version = update.OUTPUT_VERSION_FILE.read_text(encoding="utf-8").strip()

    now = datetime.now(UPDATE_LISTEN_TIMEZONE)
    start_at = now.replace(
        hour=UPDATE_LISTEN_START[0],
        minute=UPDATE_LISTEN_START[1],
        second=0,
        microsecond=0,
    )
    end_at = now.replace(
        hour=UPDATE_LISTEN_END[0],
        minute=UPDATE_LISTEN_END[1],
        second=0,
        microsecond=0,
    )
    if end_at <= start_at:
        end_at += timedelta(days=1)

    print(
        "[0/5] Listening for new APK version from "
        f"{start_at:%Y-%m-%d %H:%M} to {end_at:%Y-%m-%d %H:%M} "
        f"({UPDATE_LISTEN_TIMEZONE.key}).",
        flush=True,
    )
    if current_version:
        print(f"[0/5] Current output version: {current_version}", flush=True)

    if now < start_at or now > end_at:
        print(
            "[0/5] Current time is outside the listen window; running update directly.",
            flush=True,
        )
        update.main()
        return

    with requests.Session() as session:
        while True:
            now = datetime.now(UPDATE_LISTEN_TIMEZONE)
            if now > end_at:
                break

            try:
                info_resp = update.request_with_retry(session, update.APK_INFO_API, timeout=30)
                with info_resp:
                    info = info_resp.json()

                if not info.get("success"):
                    raise RuntimeError("Failed to fetch APK metadata")

                latest_version = str(info.get("value", {}).get("version", "")).strip()
                latest_version = latest_version.removesuffix("c")
                if latest_version:
                    print(f"[0/5] Latest upstream version: {latest_version}", flush=True)
                    if latest_version != current_version:
                        print("[0/5] New version detected.", flush=True)
                        update.OUTPUT_VERSION_FILE.write_text(
                            latest_version + "\n",
                            encoding="utf-8",
                        )
                        print("[0/5] Syncing wiki version before export.", flush=True)

                        site = pywikibot.Site("arcaea", "arcaea")
                        materialize_password_file_from_env()
                        site.login()
                        ensure_authenticated(site, "Masertwer")
                        changed = sync_pages(
                            site,
                            ensure_inputs(["Template:Version"]),
                            summary=f"Bot: sync Arcaea mobile version to {latest_version}",
                            dry_run=False,
                            minor=False,
                        )

                        print(
                            f"[0/5] Wiki version sync finished. Updated pages: {changed}",
                            flush=True,
                        )
                        print("[0/5] Continuing export.", flush=True)
                        update.main(force_refresh=True)
                        return
                else:
                    print("[0/5] Upstream metadata did not include a version.", flush=True)
            except Exception as exc:
                print(f"[0/5] Version check failed: {exc}", flush=True)

            remaining_seconds = (end_at - datetime.now(UPDATE_LISTEN_TIMEZONE)).total_seconds()
            if remaining_seconds <= 0:
                break
            time.sleep(min(UPDATE_LISTEN_POLL_SECONDS, remaining_seconds))

    print("[0/5] No new version detected before 08:10; stopping.", flush=True)
    UPDATE_SKIPPED_MARKER.write_text(
        "no new version before listen deadline\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
