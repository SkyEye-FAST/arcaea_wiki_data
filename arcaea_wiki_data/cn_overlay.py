"""Build the CN song and pack overlay consumed by Module:Arcaea/Data."""

import argparse
import json
from pathlib import Path

from arcaea_wiki_data.paths import CN_OUTPUT, INTL_OUTPUT

FILENAME = "CN_Overlay.json"
PAGE = "Module:Arcaea/CN overlay.json"


def _equal(left: object, right: object) -> bool:
    return json.dumps(left, sort_keys=True) == json.dumps(right, sort_keys=True)


def _patch(base: dict, target: dict) -> dict | None:
    """Encode field edits using the consumer's two-level merge and _delete rules."""
    if "_delete" in base or "_delete" in target:
        return None
    patch = {}
    removed = [key for key in base if key not in target]
    if removed:
        patch["_delete"] = removed
    for key, value in target.items():
        if key in base and _equal(base[key], value):
            continue
        old = base.get(key)
        if isinstance(old, dict) and isinstance(value, dict):
            if "_delete" in value:
                return None
            sub = {}
            deleted = [name for name in old if name not in value]
            if deleted:
                sub["_delete"] = deleted
            sub.update(
                {
                    name: item
                    for name, item in value.items()
                    if name not in old or not _equal(old[name], item)
                }
            )
            patch[key] = sub
        elif isinstance(old, (dict, list)) and (
            isinstance(value, dict)
            or isinstance(value, list)
            and (not value or value[0] is False or value[0] is None)
        ):
            # Lua treats an empty/false-first array as a map. Rebuild this list
            # through 'added' instead of emitting a patch the consumer would merge.
            return None
        else:
            patch[key] = value
    return patch


def _entries(data: dict, kind: str) -> tuple[list, dict]:
    entries = data.get(kind)
    if not isinstance(entries, list):
        raise ValueError(f"Missing {kind} array.")
    indexed = {}
    for item in entries:
        identifier = item.get("id") if isinstance(item, dict) else None
        if not isinstance(identifier, str) or not identifier or identifier in indexed:
            raise ValueError(f"Invalid or duplicate {kind} ID: {identifier!r}.")
        indexed[identifier] = item
    return entries, indexed


def list_overlay(base: dict, target: dict, kind: str) -> dict:
    """Encode changes while preserving the target's membership and exact order."""
    base_entries, base_map = _entries(base, kind)
    target_entries, target_map = _entries(target, kind)
    common_ids = [item["id"] for item in target_entries if item["id"] in base_map]
    added = [item for item in target_entries if item["id"] not in base_map]
    if common_ids + [item["id"] for item in added] != list(target_map):
        # The consumer appends 'added' after 'order'. Rebuilding the list also
        # represents additions interleaved with existing IDs without changing Lua.
        return {"order": [], "added": target_entries}
    patches = {}
    for identifier in common_ids:
        patch = _patch(base_map[identifier], target_map[identifier])
        if patch is None:
            return {"order": [], "added": target_entries}
        if patch:
            patches[identifier] = patch
    result = {}
    if patches:
        result["patches"] = patches
    if common_ids != [item["id"] for item in base_entries]:
        result["order"] = common_ids
    if added:
        result["added"] = added
    return result


def build_overlay(intl: Path, songs: dict, packs: dict) -> dict:
    """Compare CN tables with an explicitly selected international baseline."""
    return {
        "songs": list_overlay(json.loads((intl / "songs/songlist").read_bytes()), songs, "songs"),
        "packs": list_overlay(json.loads((intl / "songs/packlist").read_bytes()), packs, "packs"),
    }


def generate_overlay(intl: Path = INTL_OUTPUT, cn: Path = CN_OUTPUT) -> Path:
    """Regenerate the derived overlay whenever either region's tables change."""
    overlay = build_overlay(
        intl,
        json.loads((cn / "songs/songlist").read_bytes()),
        json.loads((cn / "songs/packlist").read_bytes()),
    )
    destination = cn / FILENAME
    temporary = destination.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(overlay, ensure_ascii=False, indent=4) + "\n", encoding="utf-8")
    temporary.replace(destination)
    return destination


def main() -> None:
    """Generate an overlay from existing regional exports without ADB or Wiki access."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--intl", type=Path, default=INTL_OUTPUT)
    parser.add_argument("--cn", type=Path, default=CN_OUTPUT)
    args = parser.parse_args()
    print(generate_overlay(args.intl.resolve(), args.cn.resolve()))


if __name__ == "__main__":
    main()
