"""Export Arcaea data files from game APK for wiki.arcaea.cn."""

import json
import re
import shutil
import struct
import sys
import time
import zipfile
from collections.abc import Callable
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import orjson
import requests
from fake_useragent import UserAgent

PROJECT_ROOT = Path(__file__).resolve().parent
STORY_ROOT = PROJECT_ROOT / ".arcaea-story-data" / "story"
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_PACKLIST_FILE = OUTPUT_DIR / "packlist"
OUTPUT_SONGLIST_FILE = OUTPUT_DIR / "songlist"
OUTPUT_UNLOCKS_FILE = OUTPUT_DIR / "unlocks"
OUTPUT_CHARACTERS_FILE = OUTPUT_DIR / "characters.json"
OUTPUT_VERSION_FILE = OUTPUT_DIR / "version"
OUTPUT_ARCAEA_INDEX_FILE = OUTPUT_DIR / "arcaea_index.json"
OUTPUT_ARTIST_SONG_CACHE_FILE = OUTPUT_DIR / "artist_song_cache.json"
OUTPUT_DESIGNER_SONG_CACHE_FILE = OUTPUT_DIR / "designer_song_cache.json"
OUTPUT_TL_DIR = OUTPUT_DIR / "tl"
OUTPUT_TL_JSON_FILE = OUTPUT_DIR / "tl.json"
LANGUAGES = ["zh-Hans", "zh-Hant", "en", "ja", "ko"]
LANG_KEYS = {"en": "en", "zh-Hans": "zh-hans", "zh-Hant": "zh-hant", "ja": "ja", "ko": "ko"}
APK_INFO_API = "https://webapi.lowiro.com/webapi/serve/static/bin/arcaea/apk/"
WIKI_API = "https://wiki.arcaea.cn/api.php"
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

TL_LANGUAGES = ["zh-Hans", "zh-Hant", "ja", "ko"]
ARTIST_CACHE_ZH = {"旅人E": True}
DIFFICULTY_SHORT_NAMES = {
    0: "PST",
    1: "PRS",
    2: "FTR",
    3: "BYD",
    4: "ETR",
}


def json_dumps_pretty(data: Any) -> str:
    """Dump JSON in the MediaWiki cache page style."""
    return json.dumps(data, ensure_ascii=False, indent=4) + "\n"


def po_string(keyword: str, value: str) -> list[str]:
    """Format a PO keyword as one or more string-literal lines."""
    escaped = (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace('"', '\\"')
    )

    if "\n" not in value:
        return [f'{keyword} "{escaped}"']

    lines = [f'{keyword} ""']
    parts = value.splitlines(keepends=True)
    for part in parts:
        escaped_part = (
            part.replace("\\", "\\\\")
            .replace("\t", "\\t")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
            .replace('"', '\\"')
        )
        lines.append(f'"{escaped_part}"')
    return lines


def parse_mo_entries(mo_bytes: bytes) -> list[dict[str, str]]:
    """Parse GNU gettext .mo bytes into msgid/msgstr entries."""
    if len(mo_bytes) < 28:
        raise ValueError("Invalid MO file: too short")

    magic_le = 0x950412DE
    magic_be = 0xDE120495
    magic = struct.unpack("<I", mo_bytes[:4])[0]
    if magic == magic_le:
        endian = "<"
    elif magic == magic_be:
        endian = ">"
    else:
        raise ValueError("Invalid MO file: bad magic")

    _, _, msg_count, originals_offset, translations_offset, _, _ = struct.unpack(
        f"{endian}7I", mo_bytes[:28]
    )

    entries: list[dict[str, str]] = []
    for idx in range(msg_count):
        original_len, original_offset = struct.unpack(
            f"{endian}2I", mo_bytes[originals_offset + idx * 8 : originals_offset + idx * 8 + 8]
        )
        translation_len, translation_offset = struct.unpack(
            f"{endian}2I",
            mo_bytes[translations_offset + idx * 8 : translations_offset + idx * 8 + 8],
        )

        original = mo_bytes[original_offset : original_offset + original_len].decode("utf-8")
        translation = mo_bytes[translation_offset : translation_offset + translation_len].decode(
            "utf-8"
        )

        msgctxt = ""
        msgid = original
        if "\x04" in original:
            msgctxt, msgid = original.split("\x04", 1)

        if "\x00" in msgid:
            msgid, msgid_plural = msgid.split("\x00", 1)
        else:
            msgid_plural = ""

        entry = {"msgid": msgid, "msgstr": translation}
        if msgctxt:
            entry["msgctxt"] = msgctxt
        if msgid_plural:
            entry["msgid_plural"] = msgid_plural
        entries.append(entry)

    return entries


def write_po_file(po_path: Path, entries: list[dict[str, str]]) -> None:
    """Write parsed gettext entries as a decompiled PO file."""
    lines: list[str] = []
    for entry in entries:
        if entry["msgid"] == "":
            lines.extend(po_string("msgid", entry["msgid"]))
            lines.extend(po_string("msgstr", entry["msgstr"]))
            lines.append("")
            continue

        if "msgctxt" in entry:
            lines.extend(po_string("msgctxt", entry["msgctxt"]))
        lines.extend(po_string("msgid", entry["msgid"]))
        if "msgid_plural" in entry:
            lines.extend(po_string("msgid_plural", entry["msgid_plural"]))
            for plural_index, msgstr in enumerate(entry["msgstr"].split("\x00")):
                lines.extend(po_string(f"msgstr[{plural_index}]", msgstr))
        else:
            lines.extend(po_string("msgstr", entry["msgstr"]))
        lines.append("")

    po_path.write_text("\n".join(lines), encoding="utf-8")


def extract_tl_from_apk_zip(apk_zip: zipfile.ZipFile) -> None:
    """Extract zh gettext catalogs from APK and write MO/PO/merged JSON outputs."""
    OUTPUT_TL_DIR.mkdir(parents=True, exist_ok=True)

    language_entries: dict[str, list[dict[str, str]]] = {}
    for lang in TL_LANGUAGES:
        mo_member = f"assets/tl/{lang}.mo"
        mo_bytes = apk_zip.read(mo_member)
        mo_path = OUTPUT_TL_DIR / f"{lang}.mo"
        po_path = OUTPUT_TL_DIR / f"{lang}.po"

        mo_path.write_bytes(mo_bytes)
        entries = parse_mo_entries(mo_bytes)
        write_po_file(po_path, entries)
        language_entries[lang] = entries

    merged: dict[str, dict[str, str]] = {}
    for lang, entries in language_entries.items():
        for entry in entries:
            msgid = entry["msgid"]
            if not msgid:
                continue
            key = f"{entry.get('msgctxt', '')}\x04{msgid}" if "msgctxt" in entry else msgid
            merged.setdefault(key, {})[lang] = entry["msgstr"]

    OUTPUT_TL_JSON_FILE.write_bytes(
        orjson.dumps(merged, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
    )
    print(
        f"[2/5] Extracted tl catalogs: {', '.join(TL_LANGUAGES)} ({len(merged)} merged strings).",
        flush=True,
    )


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
        parts = relative_path.parts
        is_entries_or_vn_json = (
            len(parts) == 2
            and parts[0] in {"main", "side"}
            and (parts[1].startswith("entries_") or parts[1] == "vn")
        )
        is_vns_script = len(parts) == 2 and parts[0] == "vn" and parts[1].endswith(".vns")
        if not (is_entries_or_vn_json or is_vns_script):
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


def ordered_difficulties(
    song_or_difficulties: dict[str, Any] | list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return charts ordered by ratingClass, matching Module:Arcaea/Song."""
    difficulties = (
        song_or_difficulties.get("difficulties", [])
        if isinstance(song_or_difficulties, dict)
        else song_or_difficulties
    )
    by_rating_class: dict[int, dict[str, Any]] = {}
    for chart in difficulties or []:
        rating_class = chart.get("ratingClass", chart.get("rating_class"))
        if rating_class is not None:
            by_rating_class[int(rating_class)] = chart
    return [
        by_rating_class[rating_class]
        for rating_class in range(5)
        if rating_class in by_rating_class
    ]


def difficulty_short_name(chart_or_rating_class: dict[str, Any] | int | str | None) -> str | None:
    """Return PST/PRS/FTR/BYD/ETR for a chart or rating class."""
    if isinstance(chart_or_rating_class, dict):
        rating_class = chart_or_rating_class.get(
            "ratingClass", chart_or_rating_class.get("rating_class")
        )
    else:
        rating_class = chart_or_rating_class
    if rating_class is None:
        return None
    return DIFFICULTY_SHORT_NAMES.get(int(rating_class))


def beyond_chart(song: dict[str, Any]) -> dict[str, Any] | None:
    """Return a song's BYD chart, if present."""
    for chart in song.get("difficulties", []):
        if int(chart.get("ratingClass", chart.get("rating_class", -1))) == 3:
            return chart
    return None


def load_wiki_json_page(title: str, cache_file: Path) -> dict[str, Any]:
    """Load a JSON wiki page, refreshing a local output cache when reachable."""
    params = {
        "action": "query",
        "prop": "revisions",
        "titles": title,
        "rvprop": "content",
        "rvslots": "main",
        "format": "json",
        "formatversion": "2",
    }
    try:
        with requests.Session() as session:
            response = session.get(
                WIKI_API, params=params, headers=REQUEST_HEADERS_BASE, timeout=30
            )
            response.raise_for_status()
            payload = response.json()
        pages = payload.get("query", {}).get("pages", [])
        if not pages or pages[0].get("missing"):
            raise RuntimeError(f"Missing wiki page: {title}")
        revision = pages[0].get("revisions", [{}])[0]
        slots = revision.get("slots", {})
        source = slots.get("main", {}).get("content", revision.get("content", ""))
        data = orjson.loads(source)
        cache_file.write_bytes(
            orjson.dumps(data, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)
        )
        return data
    except Exception as exc:
        if not cache_file.exists():
            raise RuntimeError(f"Unable to load {title} and no local cache exists") from exc
        print(
            f"[4/5] Failed to refresh {title}; using {cache_file.relative_to(PROJECT_ROOT)}.",
            flush=True,
        )
        return orjson.loads(cache_file.read_bytes())


def merge_song_ids(target: dict[str, Any], source: dict[str, Any], list_key: str) -> None:
    """Append song IDs from source[list_key] into target[list_key]."""
    if list_key not in source:
        return
    target.setdefault(list_key, [])
    target[list_key].extend(source[list_key])


def build_artist_single_list(
    convert_list: dict[str, dict[str, list[str]]],
    complex_artists: dict[str, Any],
) -> dict[str, dict[str, list[str]]]:
    """Expand complex artist names into per-artist song lists."""
    single_list: dict[str, dict[str, list[str]]] = {}

    def categorize(complex_artist: str, artist: str | None = None) -> None:
        target_artist = artist or complex_artist
        target = single_list.setdefault(target_artist, {})
        merge_song_ids(target, convert_list[complex_artist], "beyond")
        merge_song_ids(target, convert_list[complex_artist], "normal")

    for complex_artist in convert_list:
        artist_seen: set[str] = set()
        artist_data = complex_artists.get(complex_artist)
        if artist_data:
            full_data = artist_data.get("__FullData__") if isinstance(artist_data, dict) else None
            if full_data:
                for text in full_data:
                    artist = text.get("link")
                    if artist and artist not in artist_seen:
                        categorize(complex_artist, artist)
                        artist_seen.add(artist)
            else:
                for artist in artist_data:
                    if artist not in artist_seen:
                        categorize(complex_artist, artist)
                        artist_seen.add(artist)
        else:
            categorize(complex_artist)

    return single_list


def build_artist_song_cache(
    songlist_raw: dict[str, Any],
    version_name: str,
    complex_artists: dict[str, Any],
) -> dict[str, Any]:
    """Build Module:ArtistSong/Cache.json content from songlist."""
    data: dict[str, Any] = {}
    convert_list: dict[str, dict[str, list[str]]] = {}

    def add_artist_song(artist: str, song_id: str, list_key: str) -> None:
        convert_list.setdefault(artist, {}).setdefault(list_key, []).append(song_id)

    for song in songlist_raw.get("songs", []):
        if song.get("deleted"):
            continue
        song_id = song["id"]
        title = song.get("title_localized", {}).get("en")
        data[song_id] = {
            "title": title,
            "bpm": song.get("bpm"),
            "date": song.get("date"),
            "version": song.get("version"),
            "set": song.get("set"),
        }
        add_artist_song(song.get("artist", ""), song_id, "normal")

        byd_chart = beyond_chart(song)
        if byd_chart and (byd_chart.get("artist") or byd_chart.get("title_localized")):
            data[song_id]["byd"] = {
                "title": (byd_chart.get("title_localized") or {}).get("en"),
                "bpm": byd_chart.get("bpm"),
                "date": byd_chart.get("date"),
                "version": byd_chart.get("version"),
            }
            add_artist_song(byd_chart.get("artist") or song.get("artist", ""), song_id, "beyond")

    return {
        "ver": f"v{version_name}",
        "date": int(time.time()),
        "data": data,
        "list": build_artist_single_list(convert_list, complex_artists),
        "zh": ARTIST_CACHE_ZH,
    }


def build_designer_single_list(
    pick_list: dict[str, dict[str, dict[str, bool]]],
    designer_list_data: dict[str, Any],
    special_designers: set[str],
) -> dict[str, dict[str, dict[str, bool]]]:
    """Expand complex chart designer names into per-designer song lists."""
    complex_designers = designer_list_data.get("complex", {})
    simple_designers = designer_list_data.get("simple", [])
    single_list: dict[str, dict[str, dict[str, bool]]] = {}

    def categorize(complex_designer: str, designer: str | None = None) -> None:
        target_designer = designer or complex_designer
        target = single_list.setdefault(target_designer, {})
        for song_id, diff in pick_list[complex_designer].items():
            target[song_id] = diff

    for complex_designer in pick_list:
        designer_seen: set[str] = set()
        designer_data = complex_designers.get(complex_designer)
        if designer_data:
            full_data = (
                designer_data.get("__FullData__") if isinstance(designer_data, dict) else None
            )
            if full_data:
                for text in full_data:
                    designer = text.get("link")
                    if designer and designer not in designer_seen:
                        categorize(complex_designer, designer)
                        designer_seen.add(designer)
            else:
                for designer in designer_data:
                    if designer not in designer_seen:
                        categorize(complex_designer, designer)
                        designer_seen.add(designer)
        else:
            temp_text = complex_designer
            matched_count = 0
            for item in simple_designers:
                designer = item.get("link")
                display = item.get("display", "")
                if display and display in temp_text:
                    categorize(complex_designer, designer)
                    designer_seen.add(designer)
                    temp_text = temp_text.replace(display, "")
                    matched_count += 1
            if matched_count == 0:
                fallback = (
                    "剧情相关名义" if complex_designer in special_designers else "其他未确认名义"
                )
                categorize(complex_designer, fallback)

    return single_list


def build_designer_song_cache(
    songlist_raw: dict[str, Any],
    packlist_raw: dict[str, Any],
    version_name: str,
    designer_list_data: dict[str, Any],
) -> dict[str, Any]:
    """Build base Module:DesignerSong/Cache.json content from songlist."""
    songs = songlist_raw.get("songs", [])
    special_song = designer_list_data.get("special", {})
    byd_append = {284: "last"}

    pick_list: dict[str, dict[str, dict[str, bool]]] = {}
    song_diff_designers: dict[str, list[dict[str, Any]]] = {}
    special_designers: set[str] = set()
    byd_append_info: dict[str, dict[str, Any]] = {}

    def write_song(song_id: str, song_diff_list: list[dict[str, Any]]) -> None:
        if song_id in song_diff_designers:
            return

        song_diff_designers[song_id] = []
        same_count = 0
        last_designer: str | None = None

        for level in ordered_difficulties(song_diff_list):
            rating_class = int(level.get("ratingClass", level.get("rating_class", 0)))
            designer = level.get("chart_designer") or level.get("chartDesigner") or ""
            diff = difficulty_short_name(rating_class)
            if diff is None:
                continue
            if last_designer != designer:
                same_count = 0
                pick_list.setdefault(designer, {}).setdefault(song_id, {})[diff] = True
                song_diff_designers[song_id].append({"diff": diff, "designer": designer})
                if special_song.get(song_id):
                    special_designers.add(designer)
                last_designer = designer
            else:
                same_count += 1
                song_diff_designers[song_id].append({"diff": diff, "designer": False})
                song_diff_designers[song_id][-same_count - 1]["rowspan"] = same_count + 1

    for index, target_song_id in byd_append.items():
        source_index = index - 1
        if 0 <= source_index < len(songs):
            source_song = songs[source_index]
            chart = beyond_chart(source_song)
            if chart:
                info = dict(chart)
                info["id"] = source_song.get("id")
                byd_append_info[target_song_id] = info

    for index, song in enumerate(songs, start=1):
        if song.get("deleted"):
            continue
        song_id = song["id"]
        song_diff_list = ordered_difficulties(song)
        if song_id in byd_append_info:
            song_diff_list.append(byd_append_info[song_id])
        if index not in byd_append:
            write_song(song_id, song_diff_list)

    pack_info: dict[str, dict[str, Any]] = {
        "single": {
            "name": "Memory Archive",
            "section": "single",
            "numero": 0,
        }
    }
    for index, pack in enumerate(packlist_raw.get("packs", []), start=1):
        pack_info[pack["id"]] = {
            "_parentId_": pack.get("pack_parent"),
            "name": pack.get("name_localized", {}).get("en"),
            "section": pack.get("section"),
            "numero": index,
        }
    for item in pack_info.values():
        parent_id = item.get("_parentId_")
        if parent_id and parent_id in pack_info:
            parent = pack_info[parent_id]
            if item.get("name", "").find("Collaboration Chapter") != -1:
                item["name"] = parent.get("name", "") + " " + item.get("name", "")
            item["section"] = item.get("section") or parent.get("section")

    song_data: dict[str, dict[str, Any]] = {}
    for song in songs:
        if song.get("deleted"):
            continue
        song_id = song["id"]
        title = song.get("title_localized", {}).get("en")
        byd = beyond_chart(song)
        ftr = next(
            (
                chart
                for chart in song.get("difficulties", [])
                if int(chart.get("ratingClass", chart.get("rating_class", -1))) == 2
            ),
            None,
        )
        pack = pack_info.get(song.get("set"), {"section": "unknown", "numero": 0})
        rating = ftr and ftr.get("rating")
        rating_plus = ftr and ftr.get("ratingPlus")
        song_data[song_id] = {
            "title": title,
            "bydTitle": ((byd or {}).get("title_localized") or {}).get("en"),
            "pack": song.get("set"),
            "packName": pack.get("name"),
            "sort": {
                "section": pack.get("section") or "unknown",
                "numero": pack.get("numero") or 0,
                "rating": rating,
                "ratingPlus": bool(rating_plus),
            },
        }

    return {
        "ver": f"v{version_name}",
        "date": int(time.time()),
        "data": song_data,
        "list": build_designer_single_list(pick_list, designer_list_data, special_designers),
        "songDiffDesigner": song_diff_designers,
        "bydAppendInfo": byd_append_info,
    }


def build_song_index(songlist_raw: dict[str, Any]) -> dict[str, dict[str, int]]:
    """Build 1-based song indexes by ID and title from a songlist payload."""
    by_id: dict[str, int] = {}
    by_name: dict[str, int] = {}

    for index, song in enumerate(songlist_raw.get("songs", []), start=1):
        if song.get("deleted"):
            continue

        song_id = song.get("id")
        if song_id:
            by_id[str(song_id)] = index

        title = (song.get("title_localized") or {}).get("en")
        if title:
            by_name[str(title)] = index

    return {"id": by_id, "name": by_name}


def preserve_cache_date_if_unchanged(
    cache_data: dict[str, Any],
    output_file: Path,
) -> dict[str, Any]:
    """Keep the previous timestamp when only the generated timestamp changed."""
    if not output_file.exists():
        return cache_data

    try:
        previous_cache = orjson.loads(output_file.read_bytes())
    except Exception:
        return cache_data

    if not isinstance(previous_cache, dict):
        return cache_data

    previous_comparable = dict(previous_cache)
    current_comparable = dict(cache_data)
    previous_comparable.pop("date", None)
    current_comparable.pop("date", None)

    if previous_comparable != current_comparable:
        return cache_data

    previous_date = previous_cache.get("date")
    if previous_date is None:
        return cache_data

    preserved_cache = dict(cache_data)
    preserved_cache["date"] = previous_date
    return preserved_cache


def write_cache_outputs(
    songlist_raw: dict[str, Any],
    packlist_raw: dict[str, Any],
    version_name: str,
) -> None:
    """Write generated cache JSON files consumed by wiki modules."""
    existing_arcaea_index = load_wiki_json_page(
        "Module:Arcaea/Index.json",
        OUTPUT_ARCAEA_INDEX_FILE,
    )
    complex_artists = load_wiki_json_page(
        "Template:ComplexArtistsList.json",
        OUTPUT_DIR / "complex_artists.json",
    )
    designers_list = load_wiki_json_page(
        "Template:DesignersList.json",
        OUTPUT_DIR / "designers_list.json",
    )

    arcaea_index = dict(existing_arcaea_index)
    arcaea_index["mobile"] = build_song_index(songlist_raw)
    print(f"[5/5] Writing {OUTPUT_ARCAEA_INDEX_FILE.relative_to(PROJECT_ROOT)}...", flush=True)
    OUTPUT_ARCAEA_INDEX_FILE.write_text(
        json_dumps_pretty(arcaea_index),
        encoding="utf-8",
    )

    artist_cache = build_artist_song_cache(songlist_raw, version_name, complex_artists)
    artist_cache = preserve_cache_date_if_unchanged(
        artist_cache,
        OUTPUT_ARTIST_SONG_CACHE_FILE,
    )
    print(f"[5/5] Writing {OUTPUT_ARTIST_SONG_CACHE_FILE.relative_to(PROJECT_ROOT)}...", flush=True)
    OUTPUT_ARTIST_SONG_CACHE_FILE.write_text(
        json_dumps_pretty(artist_cache),
        encoding="utf-8",
    )

    designer_cache = build_designer_song_cache(
        songlist_raw, packlist_raw, version_name, designers_list
    )
    designer_cache = preserve_cache_date_if_unchanged(
        designer_cache,
        OUTPUT_DESIGNER_SONG_CACHE_FILE,
    )
    print(
        f"[5/5] Writing {OUTPUT_DESIGNER_SONG_CACHE_FILE.relative_to(PROJECT_ROOT)}...", flush=True
    )
    OUTPUT_DESIGNER_SONG_CACHE_FILE.write_text(
        json_dumps_pretty(designer_cache),
        encoding="utf-8",
    )


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
            headers = dict(REQUEST_HEADERS_BASE)
            try:
                headers["User-Agent"] = UserAgent().random
            except Exception:
                headers["User-Agent"] = FALLBACK_UA

            response = session.get(
                url,
                headers=headers,
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


def load_pack_song_mapping_from_apk(
    *,
    force_refresh: bool = False,
) -> tuple[dict[str, str], dict[str, str]]:
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

            version_name = str(info_value.get("version", "")).strip().removesuffix("c")
            if version_name:
                print(f"[2/5] Fetched version: {version_name}", flush=True)

                # If upstream version hasn't changed and outputs already exist,
                # skip expensive APK download/extract and reuse current output files.
                if (
                    not force_refresh
                    and current_version == version_name
                    and OUTPUT_PACKLIST_FILE.exists()
                    and OUTPUT_SONGLIST_FILE.exists()
                    and OUTPUT_UNLOCKS_FILE.exists()
                    and OUTPUT_CHARACTERS_FILE.exists()
                    and STORY_ROOT.exists()
                    and OUTPUT_TL_JSON_FILE.exists()
                    and all((OUTPUT_TL_DIR / f"{lang}.mo").exists() for lang in TL_LANGUAGES)
                    and all((OUTPUT_TL_DIR / f"{lang}.po").exists() for lang in TL_LANGUAGES)
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
                and OUTPUT_CHARACTERS_FILE.exists()
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

    with zipfile.ZipFile(BytesIO(apk_data)) as apk_zip:
        packlist_bytes = apk_zip.read("assets/songs/packlist")
        songlist_bytes = apk_zip.read("assets/songs/songlist")
        unlocks_bytes = apk_zip.read("assets/songs/unlocks")
        characters_bytes = apk_zip.read("assets/char/characters.json")
        extract_story_sources_from_apk_zip(apk_zip)
        extract_tl_from_apk_zip(apk_zip)

    packlist_raw = orjson.loads(packlist_bytes)
    songlist_raw = orjson.loads(songlist_bytes)
    characters_raw = orjson.loads(characters_bytes)
    pack_mapping, song_mapping = build_pack_song_mapping(packlist_raw, songlist_raw)

    OUTPUT_PACKLIST_FILE.write_bytes(packlist_bytes)
    OUTPUT_SONGLIST_FILE.write_bytes(songlist_bytes)
    OUTPUT_UNLOCKS_FILE.write_bytes(unlocks_bytes)
    OUTPUT_CHARACTERS_FILE.write_bytes(characters_bytes)
    print(
        f"[2/5] Extracted characters JSON: {len(characters_raw)} entries.",
        flush=True,
    )
    if version_name:
        OUTPUT_VERSION_FILE.write_text(version_name + "\n", encoding="utf-8")
        print(f"[2/5] Latest version: {version_name}", flush=True)

    print(
        f"[2/5] Loaded pack/song mappings: {len(pack_mapping)} packs, {len(song_mapping)} songs.",
        flush=True,
    )

    return pack_mapping, song_mapping


def build_story_data(
    all_stories: dict[str, dict[str, str]],
    vns_keys: set[str],
    manual_mapping: dict[str, dict[str, str]],
    pack_mapping: dict[str, str],
    song_mapping: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Build Lua story object containing metadata and per-language texts."""

    def get_pack_name(pack_id: str) -> str:
        if pack_id in pack_mapping:
            return pack_mapping[pack_id]
        if pack_id in song_mapping:
            return get_song_name(pack_id)
        return pack_id

    def get_song_name(song_id: str) -> str:
        return song_mapping.get(song_id, song_id)

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
                        str(clear_char) if clear_char is not None and clear_char != -1 else ""
                    )
                    if clear_song and clear_song.startswith("_"):
                        clear_song = None
                    clear_song_str = clear_song if clear_song else ""

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


def main(*, force_refresh: bool = False) -> None:
    """Run full Lua export pipeline."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("[0/5] Starting Lua export pipeline...", flush=True)

    manual_mapping_raw = orjson.loads((PROJECT_ROOT / "manual.json").read_bytes())
    manual_mapping: dict[str, dict[str, str]] = {}
    for k, v in manual_mapping_raw.items():
        overrides = {}
        for line in v.strip().split("\n"):
            if line.startswith("|"):
                key, val = line[1:].split("=", 1)
                overrides[key.strip()] = val.strip()
        manual_mapping[k] = overrides

    print("[1/5] Loaded local mapping files.", flush=True)

    pack_mapping, song_mapping = load_pack_song_mapping_from_apk(force_refresh=force_refresh)
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
        manual_mapping=manual_mapping,
        pack_mapping=pack_mapping,
        song_mapping=song_mapping,
    )

    print(f"[4/5] Built Lua dataset with {len(lua_story_data)} titles.", flush=True)

    songlist_raw = orjson.loads(OUTPUT_SONGLIST_FILE.read_bytes())
    packlist_raw = orjson.loads(OUTPUT_PACKLIST_FILE.read_bytes())
    version_name = OUTPUT_VERSION_FILE.read_text(encoding="utf-8").strip()
    write_cache_outputs(songlist_raw, packlist_raw, version_name)

    write_lua_outputs(lua_story_data)

    print("[5/5] Export complete.", flush=True)


if __name__ == "__main__":
    main()
