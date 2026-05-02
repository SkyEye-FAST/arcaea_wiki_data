"""Export Arcaea data files from game APK for wiki.arcaea.cn."""

import re
import shutil
import sys
import time
import zipfile
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import orjson
import requests
from fake_useragent import UserAgent

PROJECT_ROOT = Path(__file__).resolve().parent
STORY_ROOT = PROJECT_ROOT / ".arcaea-story-data" / "story"
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_PACKLIST_FILE = OUTPUT_DIR / "packlist"
OUTPUT_SONGLIST_FILE = OUTPUT_DIR / "songlist"
OUTPUT_UNLOCKS_FILE = OUTPUT_DIR / "unlocks"
OUTPUT_VERSION_FILE = OUTPUT_DIR / "version"
UPDATE_SKIPPED_MARKER = PROJECT_ROOT / ".update-skipped"
LANGUAGES = ["zh-Hans", "zh-Hant", "en", "ja", "ko"]
LANG_KEYS = {"en": "en", "zh-Hans": "zh-hans", "zh-Hant": "zh-hant", "ja": "ja", "ko": "ko"}
APK_INFO_API = "https://webapi.lowiro.com/webapi/serve/static/bin/arcaea/apk/"
UPDATE_LISTEN_TIMEZONE = ZoneInfo("Asia/Shanghai")
UPDATE_LISTEN_START = (7, 55)
UPDATE_LISTEN_END = (8, 10)
UPDATE_LISTEN_POLL_SECONDS = 30
RETRY_STATUS_CODES = {403, 429, 500, 502, 503, 504}
MAX_HTTP_RETRIES = 5
RETRY_BASE_DELAY = 1.5
REQUEST_HEADERS_BASE = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,ja;q=0.7,ko;q=0.6",
    "Connection": "keep-alive",
    "DNT": "1",
}

FALLBACK_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.7049.115 Safari/537.36"
)


def load_pack_song_mapping_from_apk_bytes(
    apk_data: bytes,
) -> tuple[dict[str, str], dict[str, str], bytes, bytes, bytes]:
    """Read pack/song data from APK bytes and extract needed story sources."""
    from io import BytesIO

    with zipfile.ZipFile(BytesIO(apk_data)) as apk_zip:
        packlist_bytes = apk_zip.read("assets/songs/packlist")
        songlist_bytes = apk_zip.read("assets/songs/songlist")
        unlocks_bytes = apk_zip.read("assets/songs/unlocks")
        extract_story_sources_from_apk_zip(apk_zip)

    packlist_raw = orjson.loads(packlist_bytes)
    songlist_raw = orjson.loads(songlist_bytes)
    pack_mapping, song_mapping = build_pack_song_mapping(packlist_raw, songlist_raw)
    return pack_mapping, song_mapping, packlist_bytes, songlist_bytes, unlocks_bytes


def should_extract_story_member(relative_path: Path) -> bool:
    """Return whether an APK app-data story member is used by this exporter."""
    parts = relative_path.parts
    if not parts:
        return False

    if parts[0] in {"main", "side"}:
        return len(parts) == 2 and (parts[1].startswith("entries_") or parts[1] == "vn")

    if parts[0] == "vn":
        return len(parts) == 2 and parts[1].endswith(".vns")

    return False


def extract_story_sources_from_apk_zip(apk_zip: zipfile.ZipFile) -> None:
    """Extract only story files consumed by this exporter from APK app-data."""
    source_prefix = "assets/app-data/story/"
    temp_story_root = STORY_ROOT.with_name(STORY_ROOT.name + ".tmp")

    shutil.rmtree(temp_story_root, ignore_errors=True)
    temp_story_root.mkdir(parents=True, exist_ok=True)

    extracted_count = 0
    for member in apk_zip.infolist():
        if member.is_dir() or not member.filename.startswith(source_prefix):
            continue

        relative_path = Path(member.filename[len(source_prefix) :])
        if not should_extract_story_member(relative_path):
            continue

        output_path = temp_story_root / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(apk_zip.read(member))
        extracted_count += 1

    if extracted_count == 0:
        raise RuntimeError("No usable story files found in APK app-data")

    shutil.rmtree(STORY_ROOT, ignore_errors=True)
    temp_story_root.replace(STORY_ROOT)
    print(f"[2/5] Extracted {extracted_count} story source files.", flush=True)


def format_wiki_text(text: str) -> str:
    """Format Arcaea-specific markup to Wiki text style."""
    if not text:
        return ""

    def convert_cg(match: Any) -> str:
        path = match.group(1)
        filename = Path(path).stem
        return f"[[文件:Story {filename} cg.jpg<WIKI_PIPE>300px]]"

    text = re.sub(r"%%CG:([^%]+)%%", convert_cg, text)
    text = re.sub(r"%%(.*?)%%\{(.*?)\}", r"{{ruby<WIKI_PIPE>\1<WIKI_PIPE>\2}}", text)
    text = re.sub(r"\^\^(.*?)\^\^\{(.*?)\}", r"{{ruby<WIKI_PIPE>\1<WIKI_PIPE>\2}}", text)
    text = re.sub(r"\$e:(.*?)\$", r"{{fc<WIKI_PIPE>darkorchid<WIKI_PIPE>\1}}", text)
    return text.strip()


def parse_json_story(
    file_path: Path, text_processor: Callable[[str], str]
) -> dict[str, dict[str, str]]:
    """Parse a main/side JSON story file."""
    if not file_path.exists():
        return {}

    data = orjson.loads(file_path.read_bytes())

    sorted_keys = sorted(
        data.keys(),
        key=lambda x: [int(c) if c.isdigit() else c for c in re.split(r"(\d+)", x)],
    )

    result: dict[str, dict[str, str]] = {}
    for key in sorted_keys:
        chapter_data = data[key]
        result[key] = {lang: text_processor(chapter_data.get(lang, "")) for lang in LANGUAGES}
    return result


def parse_vns_story_set(
    vn_dir: Path,
    text_processor: Callable[[str], str],
) -> dict[str, dict[str, str]]:
    """Parse .vns stories and return language content by base key."""
    if not vn_dir.exists():
        return {}

    files = list(vn_dir.glob("*_en.vns"))
    base_names = [f.name.replace("_en.vns", "") for f in files]

    result: dict[str, dict[str, str]] = {}
    for base in sorted(base_names):
        chapter_content: dict[str, str] = {}
        for lang in LANGUAGES:
            file_path = vn_dir / f"{base}_{lang}.vns"
            if not file_path.exists():
                chapter_content[lang] = ""
                continue

            full_text = file_path.read_text(encoding="utf-8")

            matches = re.findall(r'(?:say|say_legacy)\s+"((?:[^"\\]|\\.)*)"', full_text, re.DOTALL)
            cleaned = [text_processor(m.replace(r"\"", '"')) for m in matches]
            chapter_content[lang] = "|".join(cleaned)

        result[base] = chapter_content

    return result


def random_user_agent() -> str:
    """Generate random UA using fake-useragent, with local fallback."""
    try:
        return UserAgent().random
    except Exception:
        return FALLBACK_UA


def build_request_headers() -> dict[str, str]:
    """Generate headers that mimic a real user request profile."""
    headers = dict(REQUEST_HEADERS_BASE)
    headers["User-Agent"] = random_user_agent()
    return headers


def build_pack_song_mapping(
    packlist_raw: dict[str, Any],
    songlist_raw: dict[str, Any],
) -> tuple[dict[str, str], dict[str, str]]:
    """Build quick lookup mappings for pack/song IDs to EN names."""
    pack_mapping = {
        pack["id"]: pack.get("name_localized", {}).get("en", pack["id"])
        for pack in packlist_raw.get("packs", [])
    }
    song_mapping = {
        song["id"]: song.get("title_localized", {}).get("en", song["id"])
        for song in songlist_raw.get("songs", [])
    }
    return pack_mapping, song_mapping


def derive_apk_filename(info_value: dict[str, Any], apk_url: str) -> str:
    """Resolve APK filename from API payload/url using server-provided naming."""
    for key in ["name", "fileName", "filename"]:
        candidate = info_value.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return Path(candidate.strip()).name

    parsed = urlparse(apk_url)
    query = parse_qs(parsed.query)
    for key in ["filename", "fileName", "name"]:
        values = query.get(key)
        if values:
            candidate = values[0].strip()
            if candidate:
                return Path(candidate).name

    url_name = Path(parsed.path).name
    if url_name:
        return url_name

    version = str(info_value.get("version", "")).strip()
    version_clean = version[:-1] if version.endswith("c") else version
    if version_clean:
        return f"arcaea-{version_clean}.apk"
    return "arcaea.apk"


def request_with_retry(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    stream: bool = False,
) -> requests.Response:
    """Run GET request with retry for transient HTTP failures (including 403)."""
    last_error: Exception | None = None

    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            response = session.get(
                url,
                headers=build_request_headers(),
                timeout=timeout,
                stream=stream,
            )

            if response.status_code in RETRY_STATUS_CODES and attempt < MAX_HTTP_RETRIES:
                wait_seconds = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                print(
                    f"[2/5] HTTP {response.status_code} for {url}; retrying in {wait_seconds:.1f}s "
                    f"({attempt}/{MAX_HTTP_RETRIES})...",
                    flush=True,
                )
                response.close()
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= MAX_HTTP_RETRIES:
                break
            wait_seconds = RETRY_BASE_DELAY * (2 ** (attempt - 1))
            print(
                f"[2/5] Request failed: {exc}; retrying in {wait_seconds:.1f}s "
                f"({attempt}/{MAX_HTTP_RETRIES})...",
                flush=True,
            )
            time.sleep(wait_seconds)

    if last_error is not None:
        raise RuntimeError(f"Request failed after retries: {url}") from last_error
    raise RuntimeError(f"Request failed after retries: {url}")


def clean_version(version: str) -> str:
    """Normalize APK metadata version strings for comparison/output."""
    version = version.strip()
    return version[:-1] if version.endswith("c") else version


def fetch_latest_apk_version(session: requests.Session) -> str:
    """Fetch the latest APK version from upstream metadata."""
    info_resp = request_with_retry(session, APK_INFO_API, timeout=30)
    with info_resp:
        info = info_resp.json()

    if not info.get("success"):
        raise RuntimeError("Failed to fetch APK metadata")

    info_value = info.get("value", {})
    return clean_version(str(info_value.get("version", "")))


def wait_for_new_apk_version() -> bool:
    """Poll from 07:55 to 08:10 Asia/Shanghai until upstream version changes."""
    current_version = ""
    if OUTPUT_VERSION_FILE.exists():
        current_version = OUTPUT_VERSION_FILE.read_text(encoding="utf-8").strip()

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

    if now < start_at:
        wait_seconds = (start_at - now).total_seconds()
        print(f"[0/5] Waiting {wait_seconds:.0f}s until listen window starts...", flush=True)
        time.sleep(wait_seconds)
    elif now > end_at:
        print("[0/5] Listen window has already ended; skipping update.", flush=True)
        return False

    with requests.Session() as session:
        while True:
            now = datetime.now(UPDATE_LISTEN_TIMEZONE)
            if now > end_at:
                break

            try:
                latest_version = fetch_latest_apk_version(session)
                if latest_version:
                    print(f"[0/5] Latest upstream version: {latest_version}", flush=True)
                    if latest_version != current_version:
                        print("[0/5] New version detected; continuing export.", flush=True)
                        return True
                else:
                    print("[0/5] Upstream metadata did not include a version.", flush=True)
            except Exception as exc:
                print(f"[0/5] Version check failed: {exc}", flush=True)

            remaining_seconds = (end_at - datetime.now(UPDATE_LISTEN_TIMEZONE)).total_seconds()
            if remaining_seconds <= 0:
                break
            time.sleep(min(UPDATE_LISTEN_POLL_SECONDS, remaining_seconds))

    print("[0/5] No new version detected before 08:10; stopping.", flush=True)
    return False


def load_pack_song_mapping_from_apk() -> tuple[dict[str, str], dict[str, str]]:
    """Fetch latest APK and load packlist/songlist mapping from assets/app-data."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    apk_url: str | None = None
    version_name = ""
    current_version = ""
    apk_output_file: Path | None = None

    if OUTPUT_VERSION_FILE.exists():
        current_version = OUTPUT_VERSION_FILE.read_text(encoding="utf-8").strip()

    with requests.Session() as session:
        try:
            print("[2/5] Fetching APK metadata...", flush=True)
            info_resp = request_with_retry(session, APK_INFO_API, timeout=30)
            with info_resp:
                info = info_resp.json()

            if not info.get("success"):
                raise RuntimeError("Failed to fetch APK metadata")

            info_value = info.get("value", {})
            apk_url = str(info_value["url"])
            apk_filename = derive_apk_filename(info_value, apk_url)
            apk_output_file = PROJECT_ROOT / apk_filename

            version_name = clean_version(str(info_value.get("version", "")))
            if version_name:
                print(f"[2/5] Fetched version: {version_name}", flush=True)

                # If upstream version hasn't changed and outputs already exist,
                # skip expensive APK download/extract and reuse current output files.
                if (
                    current_version == version_name
                    and OUTPUT_PACKLIST_FILE.exists()
                    and OUTPUT_SONGLIST_FILE.exists()
                    and OUTPUT_UNLOCKS_FILE.exists()
                ):
                    print(
                        "[2/5] Version unchanged; reusing existing output data and "
                        "skipping APK download/extract.",
                        flush=True,
                    )

                    packlist_raw = orjson.loads(OUTPUT_PACKLIST_FILE.read_bytes())
                    songlist_raw = orjson.loads(OUTPUT_SONGLIST_FILE.read_bytes())
                    print(
                        "[2/5] Loaded pack/song mappings: "
                        f"{len(packlist_raw.get('packs', []))} packs, "
                        f"{len(songlist_raw.get('songs', []))} songs.",
                        flush=True,
                    )
                    return build_pack_song_mapping(packlist_raw, songlist_raw)
        except Exception as exc:
            if not (
                OUTPUT_PACKLIST_FILE.exists()
                and OUTPUT_SONGLIST_FILE.exists()
                and OUTPUT_UNLOCKS_FILE.exists()
            ):
                raise RuntimeError(
                    "Unable to get APK metadata and no local output data exists"
                ) from exc

            if OUTPUT_VERSION_FILE.exists():
                version_name = OUTPUT_VERSION_FILE.read_text(encoding="utf-8").strip()
            print(
                "[2/5] Failed to fetch latest metadata, using files in output/.",
                flush=True,
            )
            packlist_raw = orjson.loads(OUTPUT_PACKLIST_FILE.read_bytes())
            songlist_raw = orjson.loads(OUTPUT_SONGLIST_FILE.read_bytes())
            if version_name:
                print(f"[2/5] Latest version: {version_name}", flush=True)
            print(
                "[2/5] Loaded pack/song mappings: "
                f"{len(packlist_raw.get('packs', []))} packs, "
                f"{len(songlist_raw.get('songs', []))} songs.",
                flush=True,
            )

            return build_pack_song_mapping(packlist_raw, songlist_raw)

    if not apk_url:
        raise RuntimeError("No APK URL available from metadata")

    apk_data: bytes | None = None
    if apk_output_file and apk_output_file.exists():
        print(
            f"[2/5] Reusing local APK: {apk_output_file.relative_to(PROJECT_ROOT)}",
            flush=True,
        )
        apk_data = apk_output_file.read_bytes()

    if apk_data is None:
        print("[2/5] Downloading APK package...", flush=True)
        with requests.Session() as session:
            apk_resp = request_with_retry(session, apk_url, stream=True, timeout=120)
            with apk_resp:
                apk_resp.raise_for_status()
                total_size = int(apk_resp.headers.get("content-length", 0))
                downloaded = 0
                bar_width = 30
                chunks: list[bytes] = []

                if not apk_output_file:
                    apk_output_file = PROJECT_ROOT / derive_apk_filename({}, apk_url)

                for chunk in apk_resp.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    chunks.append(chunk)
                    downloaded += len(chunk)

                    if total_size > 0:
                        filled = min(bar_width, int(downloaded * bar_width / total_size))
                        percent = min(100, downloaded * 100 // total_size)
                        bar = "#" * filled + "-" * (bar_width - filled)
                        downloaded_mb = downloaded / 1024 / 1024
                        total_mb = total_size / 1024 / 1024
                        sys.stdout.write(
                            f"\r[2/5] Downloading APK package... [{bar}] {percent:3d}% "
                            f"({downloaded_mb:.1f}/{total_mb:.1f} MB)"
                        )
                    else:
                        downloaded_mb = downloaded / 1024 / 1024
                        sys.stdout.write(
                            f"\r[2/5] Downloading APK package... {downloaded_mb:.1f} MB"
                        )
                    sys.stdout.flush()

                sys.stdout.write("\n")
                sys.stdout.flush()
                apk_data = b"".join(chunks)

        if not apk_output_file:
            raise RuntimeError("Unable to determine APK output file name")
        apk_output_file.write_bytes(apk_data)
        print(f"[2/5] Saved APK: {apk_output_file.relative_to(PROJECT_ROOT)}", flush=True)

    if apk_data is None:
        raise RuntimeError("APK data is empty")

    (
        pack_mapping,
        song_mapping,
        packlist_bytes,
        songlist_bytes,
        unlocks_bytes,
    ) = load_pack_song_mapping_from_apk_bytes(apk_data)

    OUTPUT_PACKLIST_FILE.write_bytes(packlist_bytes)
    OUTPUT_SONGLIST_FILE.write_bytes(songlist_bytes)
    OUTPUT_UNLOCKS_FILE.write_bytes(unlocks_bytes)
    if version_name:
        OUTPUT_VERSION_FILE.write_text(version_name + "\n", encoding="utf-8")
        print(f"[2/5] Latest version: {version_name}", flush=True)

    print(
        f"[2/5] Loaded pack/song mappings: {len(pack_mapping)} packs, {len(song_mapping)} songs.",
        flush=True,
    )

    return pack_mapping, song_mapping


def build_manual_mapping(manual_mapping_raw: dict[str, str]) -> dict[str, dict[str, str]]:
    """Convert manual mapping text into key-value override dicts."""
    manual_mapping: dict[str, dict[str, str]] = {}
    for k, v in manual_mapping_raw.items():
        overrides = {}
        for line in v.strip().split("\n"):
            if line.startswith("|"):
                key, val = line[1:].split("=", 1)
                overrides[key.strip()] = val.strip()
        manual_mapping[k] = overrides
    return manual_mapping


def build_story_data(
    all_stories: dict[str, dict[str, str]],
    vns_keys: set[str],
    char_mapping: dict[str, str],
    manual_mapping: dict[str, dict[str, str]],
    pack_mapping: dict[str, str],
    song_mapping: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Build Lua story object containing metadata and per-language texts."""

    def get_pack_name(pack_id: str) -> str:
        if pack_id in pack_mapping:
            return pack_mapping[pack_id]
        if pack_id in song_mapping:
            return song_mapping[pack_id]
        return pack_id

    def get_song_name(song_id: str) -> str:
        return song_mapping.get(song_id, song_id)

    def get_char_name(char_id: Any) -> str:
        return char_mapping.get(str(char_id), f"Unknown ({char_id})")

    def get_title_clean(entry: dict[str, Any], major: str) -> str:
        m = entry.get("minor", 0)
        alt_p = entry.get("alternatePrefix", "")
        alt_s = entry.get("alternateSuffix", "")
        if alt_s:
            return f"{alt_p or major}-{alt_s}"
        if alt_p:
            return f"{alt_p}-{m}"
        return f"{major}-{m}"

    lua_story_data: dict[str, dict[str, Any]] = {}

    for story_dir in [STORY_ROOT / "main", STORY_ROOT / "side"]:
        print(f"[3/5] Scanning entries in {story_dir.relative_to(PROJECT_ROOT)}...", flush=True)
        entries_files = sorted(
            story_dir.glob("entries_*"),
            key=lambda x: int(x.name.split("_")[1]) if x.name.split("_")[1].isdigit() else 999,
        )

        processed_files = 0
        for entry_file in entries_files:
            major = entry_file.name.split("_")[1]
            processed_files += 1
            print(f"[3/5] Processing {entry_file.relative_to(PROJECT_ROOT)}...", flush=True)
            try:
                data = orjson.loads(entry_file.read_bytes())
            except Exception:
                continue

            if "entries" not in data or not data["entries"]:
                continue

            minor_to_title: dict[int, str] = {}
            for entry in data["entries"]:
                m = entry.get("minor", 0)
                minor_to_title[m] = get_title_clean(entry, major)

            for entry in data["entries"]:
                story_data = entry.get("storyData")
                minor = entry.get("minor", 0)

                seq_key = story_data if story_data else f"{major}-{minor}"
                keys_to_process = [seq_key]
                if entry.get("hasAlternative"):
                    keys_to_process.append(seq_key + "a")

                for key in keys_to_process:
                    if key not in all_stories:
                        continue

                    chapter_content = all_stories[key]
                    title_clean = get_title_clean(entry, major)

                    is_changed = key.endswith("a")

                    req_minor = entry.get("requiredMinor")
                    additional_requires = entry.get("additionalRequires", [])
                    req_purch = entry.get("requiredPurchase")
                    clear_char = entry.get("clearCharaId")
                    clear_song = entry.get("clearSongId")

                    req_minor_str = (
                        minor_to_title.get(req_minor, f"{major}-{req_minor}")
                        if req_minor is not None and req_minor > 0
                        else ""
                    )
                    additional_req_str = (
                        ",".join(
                            f"{r}" for r in additional_requires if r is not None and str(r) != "0"
                        )
                        if additional_requires
                        else ""
                    )
                    req_purch_str = (
                        get_pack_name(req_purch) if req_purch and req_purch != "base" else ""
                    )
                    clear_char_str = (
                        get_char_name(clear_char)
                        if clear_char is not None and clear_char != -1
                        else ""
                    )
                    if clear_song and clear_song.startswith("_"):
                        clear_song = None
                    clear_song_str = get_song_name(clear_song) if clear_song else ""

                    params: dict[str, str] = {}
                    if is_changed:
                        params["changed"] = "1"
                    if entry.get("hiddenFromCount"):
                        params["hidden"] = "1"

                    if not is_changed:
                        if entry.get("storyCgPath") or entry.get("storyType") == "vn":
                            params["hasCg"] = "1"

                        icon = entry.get("icon")
                        if icon:
                            if icon.startswith("entry_"):
                                icon = icon[6:]
                            elif icon.startswith("cell"):
                                icon = icon[5:]
                            icon = icon.replace("-", "_")
                            params["icon"] = icon

                        if req_minor_str:
                            params["requiredMinor"] = req_minor_str
                        if additional_req_str:
                            params["additionalRequires"] = additional_req_str
                        if req_purch_str:
                            params["requiredPurchase"] = req_purch_str
                        is_single_purchase = (
                            req_purch
                            and req_purch not in pack_mapping
                            and req_purch in song_mapping
                        )
                        if is_single_purchase:
                            params["singlePurchase"] = "1"
                        if clear_char_str:
                            params["clearChar"] = clear_char_str
                        if clear_song_str:
                            params["clearSong"] = clear_song_str

                    if title_clean in manual_mapping:
                        overrides = manual_mapping[title_clean]
                        params.update(overrides)
                        if "condition" in overrides:
                            params.pop("requiredMinor", None)
                            params.pop("requiredPurchase", None)
                            params.pop("singlePurchase", None)
                        if "requirement" in overrides:
                            params.pop("clearChar", None)
                            params.pop("clearSong", None)

                    if title_clean not in lua_story_data:
                        lua_story_data[title_clean] = {}

                    if not is_changed:
                        lua_story_data[title_clean]["_meta"] = params

                    lua_texts_for_chapter: dict[str, str] = {}
                    for lang in LANGUAGES:
                        raw_text = chapter_content.get(lang, "")
                        if key in vns_keys:
                            raw_text = raw_text.replace("|", "\n\n")
                        else:
                            raw_text = raw_text.replace("|", "\n----\n")
                        raw_text = raw_text.replace("<WIKI_PIPE>", "|")
                        raw_text = raw_text.replace("{{fc|", "{{color|")
                        lua_texts_for_chapter[LANG_KEYS[lang]] = raw_text.strip()

                    if not is_changed:
                        for lang_key, text in lua_texts_for_chapter.items():
                            lua_story_data[title_clean][lang_key] = text
                    else:
                        if "changed" not in lua_story_data[title_clean]:
                            lua_story_data[title_clean]["changed"] = {}
                        for lang_key, text in lua_texts_for_chapter.items():
                            lua_story_data[title_clean]["changed"][lang_key] = text

        print(
            (
                f"[3/5] Finished {story_dir.relative_to(PROJECT_ROOT)}: "
                f"{processed_files} entries files."
            ),
            flush=True,
        )

    return lua_story_data


def write_lua_outputs(lua_story_data: dict[str, dict[str, Any]]) -> None:
    """Write metadata and language Lua files to output directory."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    lua_meta_file = OUTPUT_DIR / "arcaea_story_data.lua"
    print(f"[5/5] Writing {lua_meta_file.relative_to(PROJECT_ROOT)}...", flush=True)
    with open(lua_meta_file, "w", encoding="utf-8") as out:
        out.write("return {\n")
        for title_clean, data in lua_story_data.items():
            meta = data.get("_meta", {})
            has_changed = "changed" in data
            if not meta and not has_changed:
                continue

            out.write(f'    ["{title_clean}"] = {{\n')
            for k, v in meta.items():
                escaped_v = orjson.dumps(v).decode("utf-8")
                out.write(f'        ["{k}"] = {escaped_v},\n')
            if has_changed:
                out.write('        ["changed"] = "1",\n')
            out.write("    },\n")
        out.write("}\n")

    for lk in ["zh-hans", "zh-hant", "en", "ja", "ko"]:
        lua_out_file = OUTPUT_DIR / f"arcaea_story_{lk}.lua"
        print(f"[5/5] Writing {lua_out_file.relative_to(PROJECT_ROOT)}...", flush=True)
        with open(lua_out_file, "w", encoding="utf-8") as out:
            out.write("return {\n")
            for title_clean, data in lua_story_data.items():
                text = data.get(lk, "")
                escaped_text = orjson.dumps(text).decode("utf-8")

                if "changed" in data:
                    out.write(f'    ["{title_clean}"] = {{\n')
                    out.write(f"        [1] = {escaped_text},\n")
                    changed_text = data["changed"].get(lk, "")
                    escaped_changed = orjson.dumps(changed_text).decode("utf-8")
                    out.write(f'        ["changed"] = {escaped_changed},\n')
                    out.write("    },\n")
                else:
                    out.write(f'    ["{title_clean}"] = {escaped_text},\n')
            out.write("}\n")


def main() -> None:
    """Run full Lua export pipeline."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    UPDATE_SKIPPED_MARKER.unlink(missing_ok=True)

    print("[0/5] Starting Lua export pipeline...", flush=True)

    if not wait_for_new_apk_version():
        UPDATE_SKIPPED_MARKER.write_text(
            "no new version before listen deadline\n",
            encoding="utf-8",
        )
        return

    char_mapping = orjson.loads((PROJECT_ROOT / "char_mapping.json").read_bytes())
    manual_mapping_raw = orjson.loads((PROJECT_ROOT / "manual.json").read_bytes())
    manual_mapping = build_manual_mapping(manual_mapping_raw)

    print("[1/5] Loaded local mapping files.", flush=True)

    pack_mapping, song_mapping = load_pack_song_mapping_from_apk()
    if not STORY_ROOT.exists():
        raise FileNotFoundError(f"Story root not found: {STORY_ROOT}")

    print("[3/5] Parsing story sources...", flush=True)
    main_stories = parse_json_story(STORY_ROOT / "main" / "vn", format_wiki_text)
    print(f"[3/5] Parsed main JSON stories: {len(main_stories)} chapters.", flush=True)
    side_stories = parse_json_story(STORY_ROOT / "side" / "vn", format_wiki_text)
    print(f"[3/5] Parsed side JSON stories: {len(side_stories)} chapters.", flush=True)

    vns_stories = parse_vns_story_set(STORY_ROOT / "vn", format_wiki_text)
    print(f"[3/5] Parsed VNS stories: {len(vns_stories)} chapters.", flush=True)

    all_stories = {**main_stories, **side_stories, **vns_stories}
    vns_keys = set(vns_stories.keys())

    print(f"[4/5] Building Lua dataset from {len(all_stories)} story entries...", flush=True)

    lua_story_data = build_story_data(
        all_stories=all_stories,
        vns_keys=vns_keys,
        char_mapping=char_mapping,
        manual_mapping=manual_mapping,
        pack_mapping=pack_mapping,
        song_mapping=song_mapping,
    )

    print(f"[4/5] Built Lua dataset with {len(lua_story_data)} titles.", flush=True)

    write_lua_outputs(lua_story_data)

    print("[5/5] Export complete.", flush=True)


if __name__ == "__main__":
    main()
