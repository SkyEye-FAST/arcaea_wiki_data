"""Export Arcaea story Lua files from submodule data.

This script reads story metadata/text from the local submodule and outputs:
- output/arcaea_story_data.lua
- output/arcaea_story_en.lua
- output/arcaea_story_zh-hans.lua
- output/arcaea_story_zh-hant.lua
"""

import shutil
import sys
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import orjson
import requests

PROJECT_ROOT = Path(__file__).resolve().parent
SUBMODULE_ROOT = PROJECT_ROOT / "arcaea_story"
STORY_ROOT = SUBMODULE_ROOT / "story"
OUTPUT_DIR = PROJECT_ROOT / "output"
CACHE_DIR = PROJECT_ROOT / ".cache"
APK_CACHE_FILE = CACHE_DIR / "arcaea_latest.apk"
PACKLIST_CACHE_FILE = CACHE_DIR / "packlist"
SONGLIST_CACHE_FILE = CACHE_DIR / "songlist"
LANGUAGES = ["zh-Hans", "zh-Hant", "en"]
LANG_KEYS = {"en": "en", "zh-Hans": "zh-hans", "zh-Hant": "zh-hant"}
APK_INFO_API = "https://webapi.lowiro.com/webapi/serve/static/bin/arcaea/apk/"


def format_wiki_text(text: str) -> str:
    """Format Arcaea-specific markup to Wiki text style."""
    if not text:
        return ""

    def convert_cg(match: Any) -> str:
        path = match.group(1)
        filename = Path(path).stem
        return f"[[文件:Story {filename} cg.jpg<WIKI_PIPE>300px]]"

    import re

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

    import re

    with open(file_path, encoding="utf-8") as f:
        data = orjson.loads(f.read())

    sorted_keys = sorted(
        data.keys(),
        key=lambda x: [int(c) if c.isdigit() else c for c in re.split(r"(\d+)", x)],
    )

    result: dict[str, dict[str, str]] = {}
    for key in sorted_keys:
        chapter_data = data[key]
        result[key] = {lang: text_processor(chapter_data.get(lang, "")) for lang in LANGUAGES}
    return result


def load_entries_metadata(
    entries_dir: Path, story_type: str
) -> tuple[dict[str, tuple[str, int, str, str, str]], list[str]]:
    """Load metadata from entries_<number> files."""
    metadata: dict[str, tuple[str, int, str, str, str]] = {}
    sequence: list[str] = []
    entries_files = sorted(entries_dir.glob("entries_*"), key=lambda x: int(x.name.split("_")[1]))

    for entry_file in entries_files:
        try:
            major = entry_file.name.split("_")[1]
            with open(entry_file, encoding="utf-8") as f:
                data = orjson.loads(f.read())

            if "entries" not in data:
                continue

            for entry in data["entries"]:
                story_data = entry.get("storyData")
                alternate_prefix = entry.get("alternatePrefix", "")
                minor = entry.get("minor", 0)
                alternate_suffix = entry.get("alternateSuffix", "")
                has_alternative = entry.get("hasAlternative", False)

                val = (alternate_prefix, minor, story_type, alternate_suffix, major)

                if story_data:
                    metadata[story_data] = val
                    sequence.append(story_data)
                    if has_alternative:
                        metadata[story_data + "a"] = val
                        sequence.append(story_data + "a")
                else:
                    base_key = f"{major}-{minor}"
                    sequence.append(base_key)
                    if has_alternative:
                        sequence.append(base_key + "a")
                        metadata[base_key + "a"] = val

                metadata[f"{major}-{minor}"] = val
        except (orjson.JSONDecodeError, KeyError, ValueError):
            continue

    return metadata, sequence


def parse_vns_story_set(
    vn_dir: Path,
    text_processor: Callable[[str], str],
) -> dict[str, dict[str, str]]:
    """Parse .vns stories and return language content by base key."""
    if not vn_dir.exists():
        return {}

    import re

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

            with open(file_path, encoding="utf-8") as f:
                full_text = f.read()

            matches = re.findall(r'(?:say|say_legacy)\s+"((?:[^"\\]|\\.)*)"', full_text, re.DOTALL)
            cleaned = [text_processor(m.replace(r"\"", '"')) for m in matches]
            chapter_content[lang] = "|".join(cleaned)

        result[base] = chapter_content

    return result


def load_pack_song_mapping_from_apk() -> tuple[dict[str, str], dict[str, str]]:
    """Fetch latest APK and load packlist/songlist mapping from assets/app-data."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if PACKLIST_CACHE_FILE.exists() and SONGLIST_CACHE_FILE.exists():
        print("[2/5] Loading pack/song mappings from local cache...", flush=True)
        packlist_raw = orjson.loads(PACKLIST_CACHE_FILE.read_bytes())
        songlist_raw = orjson.loads(SONGLIST_CACHE_FILE.read_bytes())
    else:
        apk_data: bytes | None = None
        if APK_CACHE_FILE.exists():
            print("[2/5] Loading APK from local cache...", flush=True)
            apk_data = APK_CACHE_FILE.read_bytes()
        else:
            print("[2/5] Fetching APK metadata...", flush=True)
            info_resp = requests.get(APK_INFO_API, timeout=30)
            info_resp.raise_for_status()
            info = info_resp.json()

            if not info.get("success"):
                raise RuntimeError("Failed to fetch APK metadata")

            apk_url = info["value"]["url"]
            print("[2/5] Downloading APK package...", flush=True)
            with requests.get(apk_url, stream=True, timeout=120) as apk_resp:
                apk_resp.raise_for_status()
                total_size = int(apk_resp.headers.get("content-length", 0))
                downloaded = 0
                bar_width = 30
                chunks: list[bytes] = []

                for chunk in apk_resp.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    chunks.append(chunk)
                    downloaded += len(chunk)

                    if total_size > 0:
                        filled = min(bar_width, int(downloaded * bar_width / total_size))
                        percent = min(100, downloaded * 100 // total_size)
                        bar = "#" * filled + "-" * (bar_width - filled)
                        sys.stdout.write(
                            f"\r[2/5] Downloading APK package... [{bar}] {percent:3d}% "
                            f"({downloaded / 1024 / 1024:.1f}/{total_size / 1024 / 1024:.1f} MB)"
                        )
                    else:
                        sys.stdout.write(
                            f"\r[2/5] Downloading APK package... {downloaded / 1024 / 1024:.1f} MB"
                        )
                    sys.stdout.flush()

                sys.stdout.write("\n")
                sys.stdout.flush()
                apk_data = b"".join(chunks)
            APK_CACHE_FILE.write_bytes(apk_data)

        from io import BytesIO

        with zipfile.ZipFile(BytesIO(apk_data)) as apk_zip:
            packlist_raw = orjson.loads(apk_zip.read("assets/songs/packlist"))
            songlist_raw = orjson.loads(apk_zip.read("assets/songs/songlist"))

        PACKLIST_CACHE_FILE.write_bytes(orjson.dumps(packlist_raw))
        SONGLIST_CACHE_FILE.write_bytes(orjson.dumps(songlist_raw))

    print(
        "[2/5] Loaded pack/song mappings: "
        f"{len(packlist_raw.get('packs', []))} packs, "
        f"{len(songlist_raw.get('songs', []))} songs.",
        flush=True,
    )

    pack_mapping = {
        pack["id"]: pack.get("name_localized", {}).get("en", pack["id"])
        for pack in packlist_raw.get("packs", [])
    }
    song_mapping = {
        song["id"]: song.get("title_localized", {}).get("en", song["id"])
        for song in songlist_raw.get("songs", [])
    }
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
                with open(entry_file, encoding="utf-8") as f:
                    data = orjson.loads(f.read())
            except Exception:
                continue

            if "entries" not in data or not data["entries"]:
                continue

            minor_to_title: dict[int, str] = {}
            for entry in data["entries"]:
                m = entry.get("minor", 0)
                alt_p = entry.get("alternatePrefix", "")
                alt_s = entry.get("alternateSuffix", "")
                if alt_s:
                    pref = alt_p if alt_p else major
                    title = f"{pref}-{alt_s}"
                elif alt_p:
                    title = f"{alt_p}-{m}"
                else:
                    title = f"{major}-{m}"
                minor_to_title[m] = title

            for entry in data["entries"]:
                story_data = entry.get("storyData")
                minor = entry.get("minor", 0)
                alt_prefix = entry.get("alternatePrefix", "")
                alt_suffix = entry.get("alternateSuffix", "")

                seq_key = story_data if story_data else f"{major}-{minor}"
                keys_to_process = [seq_key]
                if entry.get("hasAlternative"):
                    keys_to_process.append(seq_key + "a")

                for key in keys_to_process:
                    if key not in all_stories:
                        continue

                    chapter_content = all_stories[key]

                    if alt_suffix:
                        prefix = alt_prefix if alt_prefix else major
                        title_clean = f"{prefix}-{alt_suffix}"
                    elif alt_prefix:
                        title_clean = f"{alt_prefix}-{minor}"
                    else:
                        title_clean = f"{major}-{minor}"

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

    for lk in ["zh-hans", "zh-hant", "en"]:
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
    if not STORY_ROOT.exists():
        raise FileNotFoundError(f"Story root not found: {STORY_ROOT}")

    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)

    print("[0/5] Starting Lua export pipeline...", flush=True)

    with open(PROJECT_ROOT / "char_mapping.json", encoding="utf-8") as f:
        char_mapping = orjson.loads(f.read())

    with open(PROJECT_ROOT / "manual.json", encoding="utf-8") as f:
        manual_mapping_raw = orjson.loads(f.read())
    manual_mapping = build_manual_mapping(manual_mapping_raw)

    print("[1/5] Loaded local mapping files.", flush=True)

    pack_mapping, song_mapping = load_pack_song_mapping_from_apk()

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
