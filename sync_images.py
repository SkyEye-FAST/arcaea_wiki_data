"""Upload newly listed game artwork from the shared APK to the Wiki."""

import argparse
import json
import re
import tempfile
import zipfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import sync_wiki

pywikibot = sync_wiki.pywikibot
UPLOAD_SUMMARY = "{{Information Arcaea}}"
# This is the template inserted by Special:Upload's “通用” license selection.
FILE_DESCRIPTION = "{{Information Arcaea}}\n\n== 授权协议 ==\n{{Non-free fair use in}}\n"
DIFFICULTIES = {"0": "pst", "1": "prs", "2": "ftr", "3": "byd", "4": "etr"}


@dataclass(frozen=True)
class Artwork:
    """An APK member and its destination file title."""

    member: str
    title: str
    kind: str


def collect_artwork(archive: zipfile.ZipFile) -> list[Artwork]:
    """Map original full-size resources to established Wiki filenames."""
    songs = json.loads(archive.read("assets/songs/songlist"))["songs"]
    packs = json.loads(archive.read("assets/songs/packlist"))["packs"]
    song_ids = {song["id"] for song in songs}
    pack_ids = {pack["id"] for pack in packs}
    background_ids = {
        value
        for song in songs
        for entry in [song, *song.get("difficulties", [])]
        for key, value in entry.items()
        if key in {"bg", "bg_inverse"} and isinstance(value, str)
    }
    selected: dict[str, tuple[int, Artwork]] = {}

    def add(member: str, title: str, kind: str, priority: int = 0) -> None:
        # MediaWiki treats spaces and underscores identically.
        key = title.replace("_", " ")
        previous = selected.get(key)
        if previous and previous[0] == priority:
            raise ValueError(f"Ambiguous artwork for {title}: {previous[1].member}, {member}")
        if previous is None or priority > previous[0]:
            selected[key] = (priority, Artwork(member, title, kind))

    for info in archive.infolist():
        member = info.filename
        if info.is_dir():
            continue
        match = re.fullmatch(
            r"assets/songs/(?:dl_)?([^/]+)/(1080_)?(base(?:_night|_ja)?|[0-4])\.jpg",
            member,
        )
        if match:
            song_id, high_resolution, variant = match.groups()
            if song_id in song_ids:
                suffix = (
                    "" if variant == "base" else
                    "_" + DIFFICULTIES.get(variant, variant.removeprefix("base_"))
                )
                add(member, f"Songs_{song_id}{suffix}.jpg", "jacket", bool(high_resolution))
            continue
        match = re.fullmatch(
            r"assets/songs/pack/(?:1080_select_|1080/select_)([a-z0-9_]+)\.png", member
        )
        if match:
            pack_id = match[1]
            # Alternate covers are real artwork; pressed/locked buttons are not.
            if pack_id in pack_ids or (
                pack_id.endswith("_alt") and pack_id.removesuffix("_alt") in pack_ids
            ):
                add(member, f"Pack_{pack_id}.png", "pack")
            continue
        match = re.fullmatch(r"assets/char/1080/(\d+)([uol]?)\.png", member)
        icon = re.fullmatch(r"assets/char/(\d+)([uol]?)_icon\.png", member)
        if match or icon:
            character_id, variant = (match or icon).groups()
            # This icon is intentionally excluded from automatic Wiki uploads.
            if icon and character_id == "1" and variant == "l":
                continue
            suffix = {"u": "_awaken", "o": "_old", "l": "_lost", "": ""}[variant]
            if variant == "" and character_id in {"0", "1"} and not icon:
                suffix = "_new"
            if icon and variant == "o":
                suffix = ""  # Classic icons use a different suffix order.
                filename = f"Partner_{character_id}_icon_old.png"
            else:
                filename = f"Partner_{character_id}{suffix}{'_icon' if icon else ''}.png"
            add(member, filename, "icon" if icon else "partner")
            continue
        match = re.fullmatch(r"assets/img/bg/1080/([^/]+)\.(jpg|png)", member)
        if match and (match[2] == "jpg" or match[1] in background_ids):
            add(member, f"Background_{match[1]}.{match[2]}", "background")

    return [entry[1] for _, entry in sorted(selected.items())]


def missing_artwork(site, artwork: list[Artwork]) -> list[Artwork]:
    """Batch-query current file state, including redirects and shared images."""
    missing = []
    for offset in range(0, len(artwork), 50):
        batch = artwork[offset : offset + 50]
        result = site.simple_request(
            action="query", prop="imageinfo", iiprop="sha1", redirects=True,
            titles="|".join(f"File:{item.title}" for item in batch), formatversion=2,
        ).submit()
        query = result["query"]
        aliases = {
            item["from"]: item["to"]
            for key in ("normalized", "redirects") for item in query.get(key, [])
        }
        pages = {page["title"]: page for page in query["pages"]}
        for item in batch:
            title = f"File:{item.title}"
            seen = set()
            while title in aliases and title not in seen:
                seen.add(title)
                title = aliases[title]
            page = pages[title]
            if "missing" in page and not page.get("imageinfo"):
                missing.append(item)
    return missing


def new_artwork(archive: zipfile.ZipFile, baseline: Path) -> list[Artwork]:
    """Select additions against the pre-update lists, assuming old assets uploaded."""
    old_songs = {
        song["id"]: song
        for song in json.loads((baseline / "songlist").read_bytes())["songs"]
    }
    old_packs = {pack["id"] for pack in json.loads((baseline / "packlist").read_bytes())["packs"]}
    old_characters = {
        str(character["character_id"]): character
        for character in json.loads((baseline / "characters.json").read_bytes())
    }
    songs = json.loads(archive.read("assets/songs/songlist"))["songs"]
    current_songs = {song["id"]: song for song in songs}
    characters = {
        str(character["character_id"]): character
        for character in json.loads(archive.read("assets/char/characters.json"))
    }

    def backgrounds(entries):
        return {
            entry[key]
            for song in entries
            for entry in [song, *song.get("difficulties", [])]
            for key in ("bg", "bg_inverse")
            if entry.get(key)
        }

    new_backgrounds = backgrounds(songs) - backgrounds(old_songs.values())

    def overrides(song):
        return {
            str(chart["ratingClass"])
            for chart in song.get("difficulties", [])
            if chart.get("jacketOverride")
        }

    result = []
    for item in collect_artwork(archive):
        if item.kind == "jacket":
            match = re.fullmatch(
                r"assets/songs/(?:dl_)?([^/]+)/(?:1080_)?(base(?:_night|_ja)?|[0-4])\.jpg",
                item.member,
            )
            song_id, variant = match.groups()
            old_song = old_songs.get(song_id)
            added = old_song is None or (
                variant in overrides(current_songs[song_id]) - overrides(old_song)
            )
        elif item.kind == "pack":
            pack_id = item.title.removeprefix("Pack_").removesuffix(".png")
            added = pack_id not in old_packs and pack_id.removesuffix("_alt") not in old_packs
        elif item.kind in {"partner", "icon"}:
            character_id = re.match(r"Partner_(\d+)", item.title)[1]
            old_character = old_characters.get(character_id)
            added = old_character is None or (
                "_awaken" in item.title
                and not old_character.get("uncap_cores")
                and bool(characters.get(character_id, {}).get("uncap_cores"))
            )
        else:
            added = Path(item.member).stem in new_backgrounds
        if added:
            result.append(item)
    return result


def upload_artwork(site, archive: zipfile.ZipFile, pending: list[Artwork]) -> None:
    """Upload without ignoring warnings or overwriting existing files."""
    failures = []
    with tempfile.TemporaryDirectory(prefix="arcaea-wiki-images-") as directory:
        for item in pending:
            try:
                page = pywikibot.FilePage(site, f"File:{item.title}")
                # Recheck after planning to avoid overwriting a concurrent upload.
                if not missing_artwork(site, [item]):
                    print(f"[skip] {item.title}: now exists", flush=True)
                    continue
                path = Path(directory) / ("upload" + Path(item.member).suffix)
                path.write_bytes(archive.read(item.member))
                if not site.upload(
                    page, source_filename=str(path), comment=UPLOAD_SUMMARY,
                    text=FILE_DESCRIPTION, ignore_warnings=False,
                ):
                    raise RuntimeError("Upload did not succeed")
                print(f"[upload] {item.title}", flush=True)
            except Exception as exc:
                failures.append(item.title)
                print(f"[error] {item.title}: {exc}", flush=True)
    if failures:
        raise RuntimeError(f"Failed to upload {len(failures)} images: {', '.join(failures)}")


def main() -> None:
    """Plan and optionally publish missing artwork."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apk", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--baseline", type=Path, default=sync_wiki.OUTPUT_DIR,
        help="Directory containing songlist, packlist and characters.json BEFORE updating.",
    )
    args = parser.parse_args()
    with zipfile.ZipFile(args.apk) as archive:
        artwork = new_artwork(archive, args.baseline)
        print(f"New artwork: {dict(Counter(item.kind for item in artwork))}", flush=True)
        if not artwork:
            print("No additions compared with existing lists; no Wiki requests needed.", flush=True)
            return
        site = pywikibot.Site("arcaea", "arcaea")
        sync_wiki.ensure_api_available(site)
        pending = missing_artwork(site, artwork)
        print(f"Missing images: {len(pending)}; summary: {UPLOAD_SUMMARY}", flush=True)
        print(FILE_DESCRIPTION, flush=True)
        for item in pending:
            print(f"[plan] {item.member} -> File:{item.title}", flush=True)
        if args.dry_run or not pending:
            return
        sync_wiki.materialize_password_file_from_env()
        site.login()
        sync_wiki.ensure_authenticated(site, "Masertwer")
        if not site.has_right("upload") or not site.has_right("bot"):
            raise RuntimeError("Automated artwork uploads require upload and bot rights")
        upload_artwork(site, archive, pending)


if __name__ == "__main__":
    main()
