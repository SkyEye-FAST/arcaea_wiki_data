"""Exercise snapshot integrity before any history checkout is changed."""

import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from story_history import export_snapshot


class SnapshotTests(unittest.TestCase):
    """Check complete and rejected snapshots against an existing checkout."""

    def setUp(self):
        """Create an isolated history checkout."""
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repository = self.root / "history"
        (self.repository / ".git").mkdir(parents=True)
        (self.repository / "story").mkdir()
        (self.repository / "story" / "removed.ogg").write_bytes(b"obsolete")
        (self.repository / "README.md").write_text("preserve", encoding="utf-8")
        self.apk = self.root / "game.apk"

    def write_apk(self, extra=None, omit=None):
        """Write a small APK fixture with optional invalid entries."""
        files = {
            "story/main/vn": b"{}",
            "story/side/vn": b"{}",
            "story/vn/res/image.png": b"\x89PNG\x00\xff",
            "story2/ordering": b"[]",
        }
        if omit:
            del files[omit]
        files.update(extra or {})
        with zipfile.ZipFile(self.apk, "w") as archive:
            for name, contents in files.items():
                archive.writestr("assets/app-data/" + name, contents)
            archive.writestr("assets/songs/songlist", b"ignored")

    def test_complete_snapshot_removes_deleted_assets_and_preserves_binary(self):
        """Replace the whole snapshot and make repeated exports byte-identical."""
        self.write_apk()
        export_snapshot(self.apk, self.repository)
        self.assertFalse((self.repository / "story/removed.ogg").exists())
        self.assertEqual(
            (self.repository / "story/vn/res/image.png").read_bytes(), b"\x89PNG\x00\xff"
        )
        self.assertEqual((self.repository / "README.md").read_text(), "preserve")
        before = {
            p.relative_to(self.repository): p.read_bytes()
            for p in self.repository.rglob("*")
            if p.is_file()
        }
        export_snapshot(self.apk, self.repository)
        after = {
            p.relative_to(self.repository): p.read_bytes()
            for p in self.repository.rglob("*")
            if p.is_file()
        }
        self.assertEqual(before, after)

    def test_invalid_archive_never_changes_existing_history(self):
        """Reject incomplete snapshots and paths outside the content boundary."""
        for extra, omit in [
            ({"../outside": b"bad"}, None),
            ({"story/../../outside": b"bad"}, None),
            ({"story/main/./vn": b"bad"}, None),
            ({".github/workflows/injected.yml": b"bad"}, None),
            ({}, "story2/ordering"),
            ({}, "story/main/vn"),
        ]:
            with self.subTest(extra=extra, omit=omit):
                self.write_apk(extra, omit)
                with self.assertRaises(ValueError):
                    export_snapshot(self.apk, self.repository)
                self.assertEqual((self.repository / "story/removed.ogg").read_bytes(), b"obsolete")

    def test_failed_install_restores_previous_snapshot(self):
        """Restore both trees when installing the second directory fails."""
        self.write_apk()
        rename = Path.rename

        def fail_second(source, destination):
            if source.name == "story2" and source.parent.name == "new":
                raise OSError("simulated I/O failure")
            return rename(source, destination)

        with patch.object(Path, "rename", fail_second):
            with self.assertRaises(OSError):
                export_snapshot(self.apk, self.repository)
        self.assertEqual((self.repository / "story/removed.ogg").read_bytes(), b"obsolete")
        self.assertFalse((self.repository / "story/main").exists())


if __name__ == "__main__":
    unittest.main()
