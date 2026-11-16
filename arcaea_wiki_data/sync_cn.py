"""Extract the installed CN client's active data and publish its Wiki JSON pages."""

import argparse
import hashlib
import json
import re
import shlex
import subprocess
import tempfile
from pathlib import Path

from arcaea_wiki_data import cn_overlay
from arcaea_wiki_data.paths import CN_OUTPUT, INTL_OUTPUT
from arcaea_wiki_data.update import json_dumps_pretty, parse_mo_entries

DEFAULT_OUTPUT = CN_OUTPUT
PACKAGE = "com.xd.arcaea.tapcn"
ACTIVE_ROOT = f"/data/data/{PACKAGE}/files/cb/active"
TRANSLATION_SOURCE = "tl/zh-Hans_merged_automatic.mo"
SOURCES = ("songs/songlist", "songs/packlist", "songs/unlocks", TRANSLATION_SOURCE)
PAGES = {
    "Template:Songlist_CN.json": "songs/songlist",
    "Template:Packlist_CN.json": "songs/packlist",
    "Template:Translation_CN.json": "tl.json",
    cn_overlay.PAGE: cn_overlay.FILENAME,
}


class Adb:
    """Read a deliberately selected device without changing its app data."""

    def __init__(self, executable: str, serial: str) -> None:
        """Bind all device reads to the explicitly chosen ADB serial."""
        self.executable = executable
        self.serial = serial

    def run(self, *arguments: str, selected: bool = True) -> bytes:
        """Run ADB with bounded time and keep device bytes unmodified."""
        command = [self.executable]
        if selected:
            command.extend(["-s", self.serial])
        result = subprocess.run(
            [*command, *arguments],
            capture_output=True,
            timeout=60,
            check=False,
        )
        if result.returncode:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"ADB {' '.join(arguments[:2])} failed: {detail}")
        return result.stdout

    def connect(self) -> None:
        """Connect TCP devices and verify root access before reading files."""
        if ":" in self.serial:
            response = self.run("connect", self.serial, selected=False).decode(
                "utf-8", errors="replace"
            )
            if "connected to" not in response:
                raise RuntimeError(f"ADB connection failed: {response.strip()}")
        if self.run("get-state").strip() != b"device":
            raise RuntimeError(f"Device is not ready: {self.serial}")
        if b"uid=0(root)" not in self.run("exec-out", "su -c id"):
            raise RuntimeError("Root access through 'su -c' is required.")

    def version(self) -> dict[str, str]:
        """Read version metadata from the fixed CN package, never infer its region."""
        text = self.run("shell", "dumpsys", "package", PACKAGE).decode("utf-8")
        result = {}
        for key in ("versionName", "versionCode"):
            match = re.search(rf"^\s*{key}=([^\s]+)", text, flags=re.MULTILINE)
            if not match:
                raise RuntimeError(f"CN package {PACKAGE} has no {key}; is it installed?")
            result[key] = match.group(1)
        return result

    def read(self, relative_path: str) -> bytes:
        """Read one known active resource through root, without a device temp file."""
        command = shlex.join(["su", "-c", shlex.join(["cat", f"{ACTIVE_ROOT}/{relative_path}"])])
        return self.run("exec-out", command)


def validate_outputs(outputs: dict[str, bytes]) -> dict[str, int]:
    """Reject empty or malformed game data before replacing files or publishing."""
    counts = {}
    for filename, key in (
        ("songs/songlist", "songs"),
        ("songs/packlist", "packs"),
        ("songs/unlocks", "unlocks"),
    ):
        value = json.loads(outputs[filename].decode("utf-8"))
        records = value.get(key) if isinstance(value, dict) else None
        if (
            not isinstance(records, list)
            or not records
            or not all(isinstance(row, dict) for row in records)
        ):
            raise ValueError(f"{filename} must contain a nonempty {key} array of objects.")
        counts[filename] = len(records)
    translations = json.loads(outputs["tl.json"].decode("utf-8"))
    if (
        not isinstance(translations, dict)
        or not translations
        or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in translations.items()
        )
    ):
        raise ValueError("tl.json must be a nonempty string dictionary.")
    counts["tl.json"] = len(translations)
    return counts


def extract(adb: Adb, output: Path, *, intl: Path = INTL_OUTPUT) -> None:
    """Export a stable, validated CN snapshot with provenance and file hashes."""
    adb.connect()
    version = adb.version()
    resources = {path: adb.read(path) for path in SOURCES}
    # Active resources can change while the game downloads an update. Fail instead
    # of combining files from different generations; leave the previous export intact.
    if any(adb.read(path) != resources[path] for path in SOURCES) or adb.version() != version:
        raise RuntimeError(
            "CN active resources changed during extraction; finish the game update and retry."
        )
    write_snapshot(resources, version, output, intl=intl)


def write_snapshot(
    resources: dict[str, bytes],
    version: dict[str, str],
    output: Path,
    *,
    intl: Path,
) -> None:
    """Derive shared outputs from raw CN resources without storing host identity."""
    resources = dict(resources)

    translations = {}
    for entry in parse_mo_entries(resources[TRANSLATION_SOURCE]):
        if entry["msgid"]:
            key = (
                f"{entry['msgctxt']}\x04{entry['msgid']}" if "msgctxt" in entry else entry["msgid"]
            )
            translations[key] = entry["msgstr"]
    resources["tl.json"] = json_dumps_pretty(translations).encode("utf-8")
    counts = validate_outputs(resources)
    counts[TRANSLATION_SOURCE] = counts["tl.json"]
    manifest = {
        "package": PACKAGE,
        "active_root": ACTIVE_ROOT,
        **version,
        "files": {
            name: {
                "source": TRANSLATION_SOURCE if name == "tl.json" else name,
                "sha256": hashlib.sha256(content).hexdigest(),
                "bytes": len(content),
                "entries": counts[name],
            }
            for name, content in resources.items()
        },
    }
    overlay = cn_overlay.build_overlay(
        intl,
        json.loads(resources["songs/songlist"]),
        json.loads(resources["songs/packlist"]),
    )
    resources[cn_overlay.FILENAME] = json_dumps_pretty(overlay).encode("utf-8")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="cn-", dir=output.parent) as temporary:
        staging = Path(temporary)
        for name, content in resources.items():
            (staging / name).parent.mkdir(parents=True, exist_ok=True)
            (staging / name).write_bytes(content)
        (staging / "manifest.json").write_text(json_dumps_pretty(manifest), encoding="utf-8")
        output.mkdir(exist_ok=True)
        for name in [*resources, "manifest.json"]:
            (output / name).parent.mkdir(parents=True, exist_ok=True)
            (staging / name).replace(output / name)
    print(f"CN {version['versionName']} ({version['versionCode']}), {PACKAGE}")
    for name, count in counts.items():
        print(f"[export] {output / name}: {count} entries")
    print(f"[overlay] {output / cn_overlay.FILENAME}")


def import_snapshot(source: Path, output: Path, *, intl: Path = INTL_OUTPUT) -> None:
    """Validate an Android export and derive the same outputs as direct ADB extraction."""
    metadata = json.loads((source / "source.json").read_text(encoding="utf-8"))
    if metadata.get("package") != PACKAGE or metadata.get("active_root") != ACTIVE_ROOT:
        raise ValueError("Android export is not from the CN active resource directory.")
    version = {}
    for key in ("versionName", "versionCode"):
        value = metadata.get(key)
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9.+_-]+", value):
            raise ValueError(f"Android export has an invalid {key}.")
        version[key] = value
    resources = {}
    for name in SOURCES:
        content = (source / name).read_bytes()
        if hashlib.sha256(content).hexdigest() != metadata["files"][name]["sha256"]:
            raise ValueError(f"Android export hash mismatch: {name}.")
        resources[name] = content
    write_snapshot(resources, version, output, intl=intl)


def load_snapshot(directory: Path, *, intl: Path = INTL_OUTPUT) -> tuple[dict[str, Path], str]:
    """Verify every exported file before any Wiki login or edits."""
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("package") != PACKAGE or manifest.get("active_root") != ACTIVE_ROOT:
        raise ValueError("Snapshot is not from the CN active resource directory.")
    version = manifest.get("versionName")
    if not isinstance(version, str) or not version:
        raise ValueError("Snapshot is missing its CN version.")
    outputs = {}
    for name in (*SOURCES, "tl.json"):
        content = (directory / name).read_bytes()
        record = manifest["files"][name]
        source = TRANSLATION_SOURCE if name == "tl.json" else name
        if record["source"] != source:
            raise ValueError(f"Unexpected resource source for {name}; extract again.")
        if hashlib.sha256(content).hexdigest() != record["sha256"]:
            raise ValueError(f"Snapshot hash mismatch: {name}; extract again.")
        outputs[name] = content
    validate_outputs(outputs)
    cn_overlay.generate_overlay(intl, directory)
    return {title: directory / name for title, name in PAGES.items()}, version


def upload(
    directory: Path,
    *,
    wiki_url: str | None,
    user: str | None,
    dry_run: bool,
    intl: Path = INTL_OUTPUT,
) -> int:
    """Reuse the established publisher with a separate, fixed CN page mapping."""
    from arcaea_wiki_data import sync_wiki

    mapping, version = load_snapshot(directory, intl=intl)
    if wiki_url:
        from pywikibot.family import AutoFamily

        site = sync_wiki.pywikibot.Site("cnwiki", AutoFamily("cnwiki", wiki_url), user=user)
    else:
        site = sync_wiki.pywikibot.Site("arcaea", "arcaea", user=user)
    sync_wiki.ensure_api_available(site)
    print(f"Wiki target: {site.base_url(site.apipath())}")
    if not dry_run:
        sync_wiki.materialize_password_file_from_env()
        site.login()
        sync_wiki.ensure_authenticated(site, user or "Masertwer")
    return sync_wiki.sync_pages(
        site,
        mapping,
        summary=f"Bot: sync Arcaea CN {version} data",
        dry_run=dry_run,
        minor=False,
    )


def main() -> None:
    """Expose local extraction and optional upload as separate commands."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    extraction = commands.add_parser("extract", help="Read the installed CN client's active files.")
    extraction.add_argument(
        "--serial", required=True, help="Explicit ADB device serial or host:port."
    )
    extraction.add_argument(
        "--adb", default="adb", help="ADB executable (a Windows adb.exe also works in WSL)."
    )
    extraction.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    extraction.add_argument("--intl", type=Path, default=INTL_OUTPUT)
    importing = commands.add_parser("import", help="Import a directory exported inside Android.")
    importing.add_argument("--source", type=Path, required=True)
    importing.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    importing.add_argument("--intl", type=Path, default=INTL_OUTPUT)
    publication = commands.add_parser("upload", help="Publish a previously extracted CN snapshot.")
    publication.add_argument("--input", type=Path, default=DEFAULT_OUTPUT)
    publication.add_argument("--intl", type=Path, default=INTL_OUTPUT)
    publication.add_argument("--wiki-url", help="Alternate MediaWiki API endpoint.")
    publication.add_argument(
        "--user", help="Wiki username; production defaults to the existing bot config."
    )
    publication.add_argument(
        "--dry-run", action="store_true", help="Compare Wiki pages without login or edits."
    )
    args = parser.parse_args()
    try:
        if args.command == "extract":
            extract(Adb(args.adb, args.serial), args.output.resolve(), intl=args.intl.resolve())
        elif args.command == "import":
            import_snapshot(args.source.resolve(), args.output.resolve(), intl=args.intl.resolve())
        else:
            changed = upload(
                args.input.resolve(),
                wiki_url=args.wiki_url,
                user=args.user,
                dry_run=args.dry_run,
                intl=args.intl.resolve(),
            )
            print(f"{'Pending' if args.dry_run else 'Updated'} CN pages: {changed}")
    except (OSError, ValueError, RuntimeError, KeyError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"CN sync failed: {exc}\n")


if __name__ == "__main__":
    main()
