"""Sync generated output files to wiki.arcaea.cn via pywikibot."""

import argparse
import hashlib
import html
import json
import os
import re
import time
from pathlib import Path
from typing import TypeGuard

PROJECT_ROOT = Path(__file__).resolve().parent
os.environ.setdefault("PYWIKIBOT_DIR", str(PROJECT_ROOT))

import pywikibot  # noqa: E402
from pywikibot.comms import http  # noqa: E402
from pywikibot.site import BaseSite  # noqa: E402

OUTPUT_DIR = PROJECT_ROOT / "output"

ANUBIS_SCRIPT_RE = re.compile(
    r'<script\b[^>]*\bid=["\'](?P<id>anubis_[^"\']+)["\'][^>]*>'
    r"(?P<value>.*?)</script\s*>",
    re.IGNORECASE | re.DOTALL,
)
ANUBIS_PASS_PATH = "/.within.website/x/cmd/anubis/api/pass-challenge"
ANUBIS_MAX_DIFFICULTY = 6

PAGE_FILE_MAP = {
    "Module:Story/data/mobile": OUTPUT_DIR / "arcaea_story_data.lua",
    "Module:Story/data/mobile/en": OUTPUT_DIR / "arcaea_story_en.lua",
    "Module:Story/data/mobile/zh-hans": OUTPUT_DIR / "arcaea_story_zh-hans.lua",
    "Module:Story/data/mobile/zh-hant": OUTPUT_DIR / "arcaea_story_zh-hant.lua",
    "Module:Story/data/mobile/ja": OUTPUT_DIR / "arcaea_story_ja.lua",
    "Module:Story/data/mobile/ko": OUTPUT_DIR / "arcaea_story_ko.lua",
    "Template:Translation.json": OUTPUT_DIR / "tl.json",
    "Template:Version": OUTPUT_DIR / "version",
    "Template:Songlist.json": OUTPUT_DIR / "songlist",
    "Template:Packlist.json": OUTPUT_DIR / "packlist",
    "Template:Unlocks.json": OUTPUT_DIR / "unlocks",
    "Template:Characters.json": OUTPUT_DIR / "characters.json",
    "Module:Arcaea/Index.json": OUTPUT_DIR / "arcaea_index.json",
    "Module:ArtistSong/Cache.json": OUTPUT_DIR / "artist_song_cache.json",
    "Module:DesignerSong/Cache.json": OUTPUT_DIR / "designer_song_cache.json",
}

TEMPLATE_VERSION_MOBILE_RE = re.compile(
    r"(?m)^(\s*\|\s*mobile\s*=\s*\{\{\s*游戏版本\s*\|\s*)"
    r"v[^\|\}\s]+"
    r"(\s*(?:\|[^\}]*)?\}\}\s*)$"
)


def _is_string_object_dict(value: object) -> TypeGuard[dict[str, object]]:
    """Return whether a JSON value is an object with string keys."""
    return isinstance(value, dict) and all(isinstance(key, str) for key in value)


def _anubis_page_data(page_text: str) -> dict[str, object]:
    """Extract JSON values embedded in an Anubis challenge page."""
    values: dict[str, object] = {}
    for match in ANUBIS_SCRIPT_RE.finditer(page_text):
        values[match.group("id")] = json.loads(html.unescape(match.group("value")))
    return values


def _solve_anubis_pow(random_data: str, difficulty: int) -> tuple[str, int, int]:
    """Solve Anubis' SHA-256 proof of work and return hash, nonce, and time."""
    if not 1 <= difficulty <= ANUBIS_MAX_DIFFICULTY:
        raise RuntimeError(
            "Unsupported Anubis challenge difficulty "
            f"{difficulty}; expected 1-{ANUBIS_MAX_DIFFICULTY}."
        )

    started_at = time.monotonic()
    prefix = "0" * difficulty
    nonce = 0
    encoded_random_data = random_data.encode("utf-8")

    while True:
        digest = hashlib.sha256(encoded_random_data + str(nonce).encode("ascii")).hexdigest()
        if digest.startswith(prefix):
            elapsed_ms = max(1, round((time.monotonic() - started_at) * 1000))
            return digest, nonce, elapsed_ms
        nonce += 1


def ensure_api_available(site: BaseSite) -> None:
    """Pass an Anubis challenge when necessary before using the MediaWiki API."""
    response = http.request(
        site,
        uri=site.apipath(),
        params={"action": "query", "meta": "siteinfo", "format": "json"},
    )

    try:
        api_data = response.json()
    except ValueError:
        api_data = None

    if isinstance(api_data, dict):
        return

    page_data = _anubis_page_data(response.text)
    challenge_payload = page_data.get("anubis_challenge")
    if not _is_string_object_dict(challenge_payload):
        content_type = response.headers.get("content-type", "unknown")
        raise RuntimeError(
            "MediaWiki API returned a non-JSON response that is not a recognized "
            f"Anubis challenge (HTTP {response.status_code}, Content-Type: {content_type})."
        )

    rules = challenge_payload.get("rules")
    challenge = challenge_payload.get("challenge")
    if not _is_string_object_dict(rules) or not _is_string_object_dict(challenge):
        raise RuntimeError("Anubis challenge response is missing rules or challenge data.")

    algorithm = rules.get("algorithm")
    if algorithm not in {"fast", "slow"}:
        raise RuntimeError(f"Unsupported Anubis challenge algorithm: {algorithm!r}.")

    challenge_id = challenge.get("id")
    random_data = challenge.get("randomData")
    if not isinstance(challenge_id, str) or not isinstance(random_data, str):
        raise RuntimeError("Anubis challenge response is missing its id or random data.")

    difficulty_value = rules.get("difficulty")
    if isinstance(difficulty_value, bool) or not isinstance(difficulty_value, (int, str)):
        raise RuntimeError("Anubis challenge has an invalid difficulty value.")
    try:
        difficulty = int(difficulty_value)
    except ValueError as exc:
        raise RuntimeError("Anubis challenge has an invalid difficulty value.") from exc

    print(f"Anubis challenge detected; solving difficulty {difficulty} proof of work.")
    digest, nonce, elapsed_ms = _solve_anubis_pow(random_data, difficulty)

    base_prefix = page_data.get("anubis_base_prefix", "")
    if not isinstance(base_prefix, str):
        raise RuntimeError("Anubis challenge has an invalid base prefix.")
    pass_path = f"{base_prefix.rstrip('/')}{ANUBIS_PASS_PATH}"

    verified_response = http.request(
        site,
        uri=pass_path,
        params={
            "id": challenge_id,
            "response": digest,
            "nonce": nonce,
            "redir": response.url,
            "elapsedTime": elapsed_ms,
        },
    )
    try:
        verified_data = verified_response.json()
    except ValueError as exc:
        raise RuntimeError("Anubis accepted no usable API session after verification.") from exc

    if not isinstance(verified_data, dict):
        raise RuntimeError("MediaWiki API returned an unexpected response after verification.")

    print("Anubis challenge passed; MediaWiki API is available.")


def ensure_inputs(selected_pages: list[str] | None) -> dict[str, Path]:
    """Validate page selection and required local output files."""
    if selected_pages:
        unknown = [title for title in selected_pages if title not in PAGE_FILE_MAP]
        if unknown:
            known = "\n".join(f"- {title}" for title in PAGE_FILE_MAP)
            missing = "\n".join(f"- {title}" for title in unknown)
            raise ValueError(f"Unknown page titles:\n{missing}\n\nAvailable pages:\n{known}")
        mapping = {title: PAGE_FILE_MAP[title] for title in selected_pages}
    else:
        mapping = dict(PAGE_FILE_MAP)

    missing_files = [path for path in mapping.values() if not path.exists()]
    if missing_files:
        details = "\n".join(f"- {path.relative_to(PROJECT_ROOT)}" for path in missing_files)
        raise FileNotFoundError(
            f"Required output files are missing. Run update.py first.\n{details}"
        )

    return mapping


def sync_pages(
    site: BaseSite,
    mapping: dict[str, Path],
    *,
    summary: str,
    dry_run: bool,
    minor: bool,
) -> int:
    """Compare local files with wiki pages and optionally save changes."""
    changed = 0

    for title, file_path in mapping.items():
        source_text = file_path.read_text(encoding="utf-8")
        page = pywikibot.Page(site, title)
        old_text = page.text

        if title == "Template:Version":
            if not TEMPLATE_VERSION_MOBILE_RE.search(old_text):
                raise ValueError(
                    "Template:Version does not contain a recognizable mobile parameter "
                    "like '|mobile={{游戏版本|v...}}'."
                )
            new_text = TEMPLATE_VERSION_MOBILE_RE.sub(
                rf"\g<1>v{source_text.strip()}\g<2>",
                old_text,
                count=1,
            )
        else:
            new_text = source_text

        if old_text == new_text:
            print(f"[skip] {title}: no changes")
            continue

        changed += 1
        print(f"[diff] {title}: will update from {file_path.relative_to(PROJECT_ROOT)}")

        if dry_run:
            continue

        page.text = new_text
        page.save(summary=summary, minor=minor, bot=True)
        print(f"[save] {title}: updated")

    return changed


def ensure_authenticated(site: BaseSite, attempted_user: str) -> None:
    """Validate login state with userinfo and raise a helpful error if auth failed."""
    userinfo: dict = site.userinfo
    is_anon = bool(userinfo.get("anon"))
    current_user = userinfo.get("name")

    if is_anon or not current_user:
        raise RuntimeError(
            "Wiki login failed: API still reports anonymous session. "
            f"Attempted login user: {attempted_user!r}. "
            "Check whether BotPassword suffix includes a leading '@', whether "
            "the BotPassword account name is correct, and whether the password is "
            "the BotPassword token (not the normal account password)."
        )


def materialize_password_file_from_env() -> None:
    """Write user-password.cfg from env content when provided."""
    content = os.environ.get("PYWIKIBOT_PASSWORD_FILE_CONTENT")
    if content is None:
        return

    password_file = PROJECT_ROOT / "user-password.cfg"
    password_file.write_text(content, encoding="utf-8")

    try:
        password_file.chmod(0o600)
    except OSError:
        # Best-effort only; chmod may be unsupported on some platforms.
        pass


def main() -> None:
    """Run wiki sync flow using local output files and pywikibot config."""
    parser = argparse.ArgumentParser(
        description="Upload output files to wiki.arcaea.cn with pywikibot.",
    )
    parser.add_argument(
        "--summary",
        default="Bot: sync Arcaea story data",
        help="Edit summary used for all page updates.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show pending changes but do not write to wiki.",
    )
    parser.add_argument(
        "--page",
        action="append",
        dest="pages",
        help="Optional page title filter; repeatable.",
    )
    parser.add_argument(
        "--minor",
        action="store_true",
        help="Mark edits as minor edits.",
    )
    args = parser.parse_args()
    mapping = ensure_inputs(args.pages)

    site = pywikibot.Site("arcaea", "arcaea")
    ensure_api_available(site)
    if not args.dry_run:
        materialize_password_file_from_env()

        site.login()
        ensure_authenticated(site, "Masertwer")
    else:
        print("Dry-run mode: skip login and do not write edits.")

    changed = sync_pages(
        site,
        mapping,
        summary=args.summary,
        dry_run=args.dry_run,
        minor=args.minor,
    )

    if args.dry_run:
        print(f"Dry-run finished. Pending updates: {changed}")
    else:
        print(f"Sync finished. Updated pages: {changed}")


if __name__ == "__main__":
    main()
