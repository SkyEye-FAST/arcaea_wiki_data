"""Check that producer retries do not depend on Wiki publication state."""

import tempfile
import unittest
import zipfile
from datetime import datetime as real_datetime
from pathlib import Path
from unittest.mock import Mock, patch

import listen_update


class AcquisitionTests(unittest.TestCase):
    """Exercise acquisition with fake network responses and a fixed clock."""

    def test_unchanged_version_is_downloaded_and_metadata_is_read_once(self):
        """Allow same-version revisions and retry both publishers from one APK."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            version_file = root / "generated-version"
            version_file.write_text("7.0.255\n", encoding="utf-8")
            fixture = root / "fixture.apk"
            with zipfile.ZipFile(fixture, "w") as archive:
                archive.writestr("example", b"data")
            response = Mock()
            response.iter_content.return_value = [fixture.read_bytes()]
            context = Mock()
            context.__enter__ = Mock(return_value=response)
            context.__exit__ = Mock(return_value=False)
            with (
                patch.object(listen_update, "DOWNLOAD_DIR", root / "pipeline"),
                patch.object(listen_update.update, "OUTPUT_VERSION_FILE", version_file),
                patch.object(listen_update, "datetime") as clock,
                patch.object(
                    listen_update,
                    "fetch_metadata",
                    return_value=("7.0.255", "https://example.com/game.apk"),
                ) as metadata,
                patch.object(
                    listen_update.update, "request_with_retry", return_value=context
                ) as download,
                patch.object(listen_update, "publish_version") as early_notice,
            ):
                clock.now.return_value = real_datetime(2026, 9, 10, 9, 0)
                apk, version = listen_update.acquire()
                self.assertEqual(apk.read_bytes(), fixture.read_bytes())
                self.assertEqual(version, "7.0.255")
                metadata.assert_called_once()
                download.assert_called_once()
                early_notice.assert_not_called()
            self.assertEqual(version_file.read_text(), "7.0.255\n")


if __name__ == "__main__":
    unittest.main()
