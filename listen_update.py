"""Acquire one APK for the Wiki and story history publishers."""

import json
import os
import subprocess
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

import update

DOWNLOAD_DIR = update.PROJECT_ROOT / ".pipeline"
TIMEZONE = ZoneInfo("Asia/Shanghai")
LISTEN_START = (7, 50)
LISTEN_END = (8, 30)
POLL_SECONDS = 10


def fetch_metadata(session: requests.Session) -> tuple[str, str]:
    """Read the version and URL from the same metadata response."""
    with update.request_with_retry(session, update.APK_INFO_API, timeout=30) as response:
        info = response.json()
    if not info.get("success"):
        raise RuntimeError("Failed to fetch APK metadata")
    version = str(info["value"]["version"]).strip().removesuffix("c")
    url = str(info["value"]["url"])
    if not version or any(character not in "0123456789." for character in version):
        raise ValueError("Invalid APK version")
    if not url.startswith("https://"):
        raise ValueError("APK URL must use HTTPS")
    return version, url


def publish_version(version: str) -> None:
    """Publish the early version notice without changing generated output state."""
    version_file = DOWNLOAD_DIR / "version"
    version_file.write_text(version + "\n", encoding="utf-8")
    try:
        result = subprocess.run(
            [
                sys.executable,
                "sync_wiki.py",
                "--version-file",
                str(version_file),
                "--summary",
                f"Bot: sync Arcaea mobile version to {version}",
            ],
            cwd=update.PROJECT_ROOT,
            check=False,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        print("Early Wiki version notice timed out; continuing acquisition.", flush=True)
        return
    if result.returncode:
        print("Early Wiki version notice failed; the full Wiki sync will retry it.", flush=True)


def acquire() -> tuple[Path, str]:
    """Listen during the release window, then download and validate one APK."""
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    current_version = (
        update.OUTPUT_VERSION_FILE.read_text(encoding="utf-8").strip()
        if update.OUTPUT_VERSION_FILE.exists()
        else ""
    )
    with requests.Session() as session:
        while True:
            version, url = fetch_metadata(session)
            now = datetime.now(TIMEZONE)
            in_window = LISTEN_START <= (now.hour, now.minute) < LISTEN_END
            if version != current_version:
                if in_window:
                    publish_version(version)
                break
            if not in_window:
                break
            print(f"Waiting for a release after {current_version}...", flush=True)
            time.sleep(POLL_SECONDS)

        # Inspect unchanged versions too: publishers may need retries, and upstream
        # can revise story content without changing its version number.
        apk_path = DOWNLOAD_DIR / "game.apk"
        partial_path = DOWNLOAD_DIR / "game.apk.part"
        with update.request_with_retry(session, url, timeout=120, stream=True) as response:
            with partial_path.open("wb") as destination:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    destination.write(chunk)
        with zipfile.ZipFile(partial_path) as archive:
            corrupt = archive.testzip()
            if corrupt:
                raise ValueError(f"Corrupt APK entry: {corrupt}")
        partial_path.replace(apk_path)
    (DOWNLOAD_DIR / "source.json").write_text(
        json.dumps({"version": version, "url": url}, indent=2) + "\n", encoding="utf-8"
    )
    return apk_path, version


if __name__ == "__main__":
    apk, version = acquire()
    if output := os.environ.get("GITHUB_OUTPUT"):
        with Path(output).open("a", encoding="utf-8") as stream:
            stream.write(f"apk={apk}\nversion={version}\n")
    print(f"Acquired APK version {version}: {apk}", flush=True)
