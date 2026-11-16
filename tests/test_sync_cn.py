"""Check CN resource provenance, failed extraction, and publication boundaries."""

import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from arcaea_wiki_data import sync_cn as cn
from arcaea_wiki_data import sync_wiki


def catalog() -> bytes:
    """Build a tiny real gettext catalog, including a context-specific entry."""
    entries = [
        (b"", b"Content-Type: text/plain; charset=UTF-8\n"),
        (b"Act I", "第I幕".encode()),
        (b"menu\x04Start", "开始".encode()),
    ]
    count = len(entries)
    offset = 28 + 16 * count
    originals, translations, payload = [], [], b""
    for original, _ in entries:
        originals.append((len(original), offset + len(payload)))
        payload += original + b"\0"
    for _, translated in entries:
        translations.append((len(translated), offset + len(payload)))
        payload += translated + b"\0"
    header = struct.pack("<7I", 0x950412DE, 0, count, 28, 28 + 8 * count, 0, 0)
    return (
        header + b"".join(struct.pack("<2I", *item) for item in originals + translations) + payload
    )


class CnTests(unittest.TestCase):
    """Exercise an extraction and the same snapshot's later publication."""

    def setUp(self):
        """Provide a small, independent international baseline for each test."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.intl = Path(temporary.name)
        (self.intl / "songs").mkdir()
        (self.intl / "songs/songlist").write_text('{"songs":[{"id":"example"}]}')
        (self.intl / "songs/packlist").write_text('{"packs":[{"id":"example"}]}')

    def device(self) -> Mock:
        """Represent the CN client with distinct, stable resource bytes."""
        resources = {
            "songs/songlist": b'{"songs": [{"id":"example"}]}\n',
            "songs/packlist": b'{"packs": [{"id":"example"}]}\n',
            "songs/unlocks": b'{"unlocks": [{"songId":"example"}]}\n',
            "tl/zh-Hans_merged_automatic.mo": catalog(),
        }
        device = Mock(serial="127.0.0.1:16480")
        device.version.return_value = {"versionName": "7.2.0", "versionCode": "285"}
        device.read.side_effect = resources.__getitem__
        return device

    def test_export_round_trip(self):
        """Preserve game JSON bytes and export the merged CN translations."""
        device = self.device()
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            cn.extract(device, directory, intl=self.intl)
            mapping, version = cn.load_snapshot(directory, intl=self.intl)
            self.assertEqual(version, "7.2.0")
            self.assertEqual(set(mapping), set(cn.PAGES))
            self.assertIn("Module:Arcaea/CN overlay.json", mapping)
            self.assertNotIn("Template:Unlocks_CN.json", mapping)
            self.assertEqual(
                (directory / "songs/songlist").read_bytes(), device.read("songs/songlist")
            )
            self.assertEqual(
                json.loads((directory / "tl.json").read_text()),
                {"Act I": "第I幕", "menu\x04Start": "开始"},
            )
            manifest = json.loads((directory / "manifest.json").read_text())
            self.assertNotIn("serial", manifest)
            self.assertEqual(
                manifest["files"]["tl.json"]["source"], "tl/zh-Hans_merged_automatic.mo"
            )
            self.assertEqual((directory / cn.TRANSLATION_SOURCE).read_bytes(), catalog())

    def test_corrupt_resource_preserves_previous_export(self):
        """A read or validation failure must leave the last complete export usable."""
        device = self.device()
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            cn.extract(device, directory, intl=self.intl)
            previous = {
                path.relative_to(directory): path.read_bytes()
                for path in directory.rglob("*")
                if path.is_file()
            }
            read = device.read.side_effect
            device.read.side_effect = lambda path: (
                b'{"songs": []}' if path == "songs/songlist" else read(path)
            )
            with self.assertRaisesRegex(ValueError, "nonempty songs"):
                cn.extract(device, directory, intl=self.intl)
            self.assertEqual(
                previous,
                {
                    path.relative_to(directory): path.read_bytes()
                    for path in directory.rglob("*")
                    if path.is_file()
                },
            )

    def test_changing_active_resources_rejected(self):
        """An update during extraction must never produce a mixed snapshot."""
        device = self.device()
        read = device.read.side_effect
        reads = 0

        def changing(path):
            nonlocal reads
            reads += 1
            return read(path) + (b" " if reads > len(cn.SOURCES) else b"")

        device.read.side_effect = changing
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "snapshot"
            with self.assertRaisesRegex(RuntimeError, "changed during extraction"):
                cn.extract(device, output, intl=self.intl)
            self.assertFalse(output.exists())

    def test_tampered_snapshot_rejected_before_login(self):
        """Uploading a modified or interrupted export fails before contacting a Wiki."""
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            cn.extract(self.device(), directory, intl=self.intl)
            (directory / "songs/songlist").write_bytes(b'{"songs":[]}')
            with patch.object(sync_wiki.pywikibot, "Site") as site:
                with self.assertRaisesRegex(ValueError, "hash mismatch"):
                    cn.upload(directory, wiki_url=None, user=None, dry_run=False, intl=self.intl)
                site.assert_not_called()

    def test_android_import_matches_adb_export(self):
        """A verified Android export produces exactly the direct extraction snapshot."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "android"
            source.mkdir()
            device = self.device()
            metadata = {
                "package": cn.PACKAGE,
                "active_root": cn.ACTIVE_ROOT,
                **device.version(),
                "files": {},
            }
            for name in cn.SOURCES:
                content = device.read(name)
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
                metadata["files"][name] = {"sha256": hashlib.sha256(content).hexdigest()}
            (source / "source.json").write_text(json.dumps(metadata))
            cn.import_snapshot(source, root / "imported", intl=self.intl)
            cn.extract(device, root / "direct", intl=self.intl)
            for path in (root / "direct").rglob("*"):
                if path.is_file():
                    self.assertEqual(
                        path.read_bytes(),
                        (root / "imported" / path.relative_to(root / "direct")).read_bytes(),
                    )
            (source / "songs/songlist").write_bytes(b'{"songs":[]}')
            with self.assertRaisesRegex(ValueError, "Android export hash mismatch"):
                cn.import_snapshot(source, root / "rejected", intl=self.intl)
            self.assertFalse((root / "rejected").exists())

    def test_publication_refreshes_overlay_after_baseline_change(self):
        """A verified snapshot must produce a fresh overlay for the selected baseline."""
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            cn.extract(self.device(), directory, intl=self.intl)
            before = (directory / "CN_Overlay.json").read_bytes()
            (self.intl / "songs/songlist").write_text(
                '{"songs":[{"id":"example","new_intl_field":true}]}'
            )
            cn.load_snapshot(directory, intl=self.intl)
            after = json.loads((directory / "CN_Overlay.json").read_bytes())
            self.assertNotEqual(before, (directory / "CN_Overlay.json").read_bytes())
            self.assertEqual(after["songs"]["patches"]["example"]["_delete"], ["new_intl_field"])

    def test_fixed_package_version_and_root(self):
        """Use the installed CN package's metadata and require root explicitly."""
        device = cn.Adb("adb", "127.0.0.1:16480")
        with patch.object(
            device, "run", return_value=b"  versionName=7.2.0\n  versionCode=285 minSdk=24\n"
        ) as run:
            self.assertEqual(device.version(), {"versionName": "7.2.0", "versionCode": "285"})
            run.assert_called_once_with("shell", "dumpsys", "package", cn.PACKAGE)
        with patch.object(
            device,
            "run",
            side_effect=[b"connected to 127.0.0.1:16480", b"device\n", b"uid=2000(shell)"],
        ):
            with self.assertRaisesRegex(RuntimeError, "Root access"):
                device.connect()

    def test_failed_tcp_connection_without_adb_error_code(self):
        """ADB can report a failed TCP connection while returning exit code zero."""
        device = cn.Adb("adb", "127.0.0.1:16480")
        with patch.object(device, "run", return_value=b"cannot connect to 127.0.0.1:16480") as run:
            with self.assertRaisesRegex(RuntimeError, "ADB connection failed"):
                device.connect()
            run.assert_called_once_with("connect", device.serial, selected=False)

    def test_json_formatting_does_not_create_edits(self):
        """MediaWiki's JSON whitespace normalization must not cause repeat edits."""
        page = Mock(text='{\n\t"songs": [{"id": "example"}]\n}')
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "songlist"
            source.write_text('{"songs":[{"id":"example"}]}')
            with patch.object(sync_wiki.pywikibot, "Page", return_value=page):
                changed = sync_wiki.sync_pages(
                    Mock(),
                    {"Template:Songlist_CN.json": source},
                    summary="CN",
                    dry_run=False,
                    minor=False,
                )
            self.assertEqual(changed, 0)
            page.save.assert_not_called()

    def test_dry_run_and_new_json_page(self):
        """Dry runs perform no edits; newly created data tables use the JSON model."""
        page = Mock(text="")
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "songlist"
            source.write_text('{"songs":[{"id":"example"}]}')
            mapping = {"Template:Songlist_CN.json": source}
            with patch.object(sync_wiki.pywikibot, "Page", return_value=page):
                sync_wiki.sync_pages(Mock(), mapping, summary="CN", dry_run=True, minor=False)
                page.save.assert_not_called()
                sync_wiki.sync_pages(Mock(), mapping, summary="CN", dry_run=False, minor=False)
                page.save.assert_called_once_with(
                    summary="CN", minor=False, bot=True, contentmodel="json"
                )


if __name__ == "__main__":
    unittest.main()
