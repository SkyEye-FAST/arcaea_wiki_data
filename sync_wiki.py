"""Sync generated output files to wiki.arcaea.cn via pywikibot."""

import argparse
import os
import re
from pathlib import Path

import pywikibot
from pywikibot.login import ClientLoginManager
from pywikibot.site import BaseSite

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "output"

PAGE_FILE_MAP = {
    "Module:Story/data": OUTPUT_DIR / "arcaea_story_data.lua",
    "Module:Story/data/en": OUTPUT_DIR / "arcaea_story_en.lua",
    "Module:Story/data/zh-hans": OUTPUT_DIR / "arcaea_story_zh-hans.lua",
    "Module:Story/data/zh-hant": OUTPUT_DIR / "arcaea_story_zh-hant.lua",
    "Module:Story/data/ja": OUTPUT_DIR / "arcaea_story_ja.lua",
    "Module:Story/data/ko": OUTPUT_DIR / "arcaea_story_ko.lua",
    "Template:Version": OUTPUT_DIR / "version",
    "Template:Songlist.json": OUTPUT_DIR / "songlist",
    "Template:Packlist.json": OUTPUT_DIR / "packlist",
    "Template:Unlocks.json": OUTPUT_DIR / "unlocks",
}


def update_template_version_mobile_only(old_text: str, version_text: str) -> str:
    """Update only the mobile version value in Template:Version content."""
    version = version_text.strip()
    pattern = re.compile(
        r"(?m)^(\s*\|\s*mobile\s*=\s*\{\{\s*游戏版本\s*\|\s*)"
        r"v[^\|\}\s]+"
        r"(\s*(?:\|[^\}]*)?\}\}\s*)$"
    )

    if not pattern.search(old_text):
        raise ValueError(
            "Template:Version does not contain a recognizable mobile parameter "
            "like '|mobile={{游戏版本|v...}}'."
        )

    return pattern.sub(rf"\g<1>v{version}\g<2>", old_text, count=1)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments for the sync script."""
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
    return parser.parse_args()


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
            new_text = update_template_version_mobile_only(old_text, source_text)
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
        page.save(summary=summary, minor=minor)
        print(f"[save] {title}: updated")

    return changed


def ensure_authenticated(site: BaseSite, attempted_user: str) -> None:
    """Validate login state with userinfo and raise a helpful error if auth failed."""
    userinfo = site.userinfo
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


def main() -> None:
    """Run wiki sync flow using local output files and pywikibot config."""
    args = parse_args()
    mapping = ensure_inputs(args.pages)

    site = pywikibot.Site("arcaea", "arcaea")
    if not args.dry_run:
        username = (os.environ.get("PYWIKIBOT_USERNAME") or "").strip()
        password = (os.environ.get("PYWIKIBOT_PASSWORD") or "").strip()
        suffix = (os.environ.get("PYWIKIBOT_BOTPASSWORD_SUFFIX") or "").strip().lstrip("@")

        if password:
            if not username:
                raise ValueError("PYWIKIBOT_USERNAME is required when PYWIKIBOT_PASSWORD is set")

            login_user = username
            if suffix and "@" not in username:
                login_user = f"{username}@{suffix}"

            pywikibot.config.password_file = None  # type: ignore
            login_manager = ClientLoginManager(site=site, user=login_user, password=password)
            login_manager.login()
            ensure_authenticated(site, login_user)
        else:
            site.login()
            ensure_authenticated(site, username or "<from-config>")
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
