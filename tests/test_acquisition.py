"""Check acquisition and the version signal controlling Wiki publication."""

import os
import tempfile
import unittest
import zipfile
from datetime import datetime as real_datetime
from pathlib import Path
from unittest.mock import Mock, patch

from arcaea_wiki_data import listen_update


class AcquisitionTests(unittest.TestCase):
    """Exercise acquisition with fake network responses and a fixed clock."""

    def test_acquisition_reports_changes_against_generated_version(self):
        """Keep story acquisition active while gating Wiki saves by version."""
        outside_window = real_datetime(2026, 9, 10, 9, 0)
        in_window = real_datetime(2026, 9, 10, 7, 54)
        deadline = real_datetime(2026, 9, 10, 8, 30)
        cases = [
            ("7.0.255\n", ["7.0.255"], [outside_window], False, False),
            ("7.0.254\n", ["7.0.255"], [outside_window], True, False),
            (None, ["7.0.255"], [outside_window], True, False),
            ("7.0.254\n", ["7.0.255"], [in_window], True, True),
            (
                "7.0.254\n",
                ["7.0.254", "7.0.255"],
                [in_window, in_window],
                True,
                True,
            ),
            (
                "7.0.255\n",
                ["7.0.255", "7.0.255"],
                [in_window, deadline],
                False,
                False,
            ),
        ]
        for baseline, versions, times, changed, notice in cases:
            with (
                self.subTest(baseline=baseline, versions=versions, times=times),
                tempfile.TemporaryDirectory() as temporary,
            ):
                self.check_acquisition(temporary, baseline, versions, times, changed, notice)

    def check_acquisition(self, temporary, baseline, versions, times, changed, notice):
        """Acquire a fake APK without contacting the Wiki or download service."""
        root = Path(temporary)
        version_file = root / "generated-version"
        if baseline is not None:
            version_file.write_text(baseline, encoding="utf-8")
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
                side_effect=[(version, "https://example.com/game.apk") for version in versions],
            ) as metadata,
            patch.object(
                listen_update.update, "request_with_retry", return_value=context
            ) as download,
            patch.object(listen_update, "publish_version") as early_notice,
            patch.object(listen_update.time, "sleep") as sleep,
        ):
            clock.now.side_effect = times
            apk, version, version_changed = listen_update.acquire()
            self.assertEqual(apk.read_bytes(), fixture.read_bytes())
            self.assertEqual(version, versions[-1])
            self.assertIs(version_changed, changed)
            self.assertEqual(metadata.call_count, len(versions))
            self.assertEqual(sleep.call_count, len(versions) - 1)
            download.assert_called_once()
            if notice:
                early_notice.assert_called_once_with(versions[-1])
            else:
                early_notice.assert_not_called()
        if baseline is None:
            self.assertFalse(version_file.exists())
        else:
            self.assertEqual(version_file.read_text(), baseline)

    def test_actions_outputs_include_explicit_version_change(self):
        """GitHub Actions receives lowercase boolean strings for its conditions."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for changed, expected in [(False, "false"), (True, "true")]:
                output = root / f"output-{expected}"
                output.write_text("existing=value\n", encoding="utf-8")
                with (
                    self.subTest(changed=changed),
                    patch.object(
                        listen_update,
                        "acquire",
                        return_value=(root / "game.apk", "7.0.255", changed),
                    ),
                    patch.dict(os.environ, {"GITHUB_OUTPUT": str(output)}),
                ):
                    listen_update.main()
                self.assertEqual(
                    output.read_text(),
                    f"existing=value\napk={root / 'game.apk'}\nversion=7.0.255\n"
                    f"version_changed={expected}\n",
                )


if __name__ == "__main__":
    unittest.main()
