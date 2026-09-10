"""Export complete APK story snapshots into a checked-out history repository."""

import argparse
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

STORY_DIRECTORIES = ("story", "story2")
APK_PREFIX = "assets/app-data/"


def export_snapshot(apk: Path, repository: Path) -> None:
    """Validate a complete snapshot before replacing the managed directories."""
    repository = repository.resolve()
    if not (repository / ".git").exists():
        raise ValueError("The destination must be a Git checkout")
    for name in STORY_DIRECTORIES:
        target = repository / name
        if target.is_symlink() or target.resolve().parent != repository:
            raise ValueError(f"Unsafe destination: {name}")

    with tempfile.TemporaryDirectory(prefix=".snapshot-", dir=repository) as temporary:
        staging = Path(temporary) / "new"
        backup = Path(temporary) / "old"
        backup.mkdir()
        with zipfile.ZipFile(apk) as archive:
            seen: set[str] = set()
            for member in archive.infolist():
                if not member.filename.startswith(APK_PREFIX) or member.is_dir():
                    continue
                relative = member.filename.removeprefix(APK_PREFIX)
                path = PurePosixPath(relative)
                if (
                    path.is_absolute()
                    or ".." in path.parts
                    or "\\" in relative
                    or ":" in relative
                    or not path.parts
                    or path.parts[0] not in STORY_DIRECTORIES
                    or path.as_posix() != relative
                    or relative in seen
                ):
                    raise ValueError(f"Unexpected APK story path: {relative}")
                seen.add(relative)
                destination = staging.joinpath(*path.parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(archive.read(member))

        for name in STORY_DIRECTORIES:
            if not (staging / name).is_dir():
                raise ValueError(f"APK is missing the {name} snapshot")
        for required in ("story/main/vn", "story/side/vn", "story2/ordering"):
            if not (staging / required).is_file():
                raise ValueError(f"APK is missing {required}")

        installed: list[str] = []
        saved: list[str] = []
        try:
            for name in STORY_DIRECTORIES:
                destination = repository / name
                if destination.exists():
                    destination.rename(backup / name)
                    saved.append(name)
                (staging / name).rename(destination)
                installed.append(name)
        except OSError:
            for name in reversed(installed):
                (repository / name).rename(staging / name)
            for name in reversed(saved):
                (backup / name).rename(repository / name)
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apk", type=Path, required=True)
    parser.add_argument("--repository", type=Path, required=True)
    arguments = parser.parse_args()
    export_snapshot(arguments.apk, arguments.repository)
