"""Verify international APK extraction into the regional resource layout."""

import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from arcaea_wiki_data import update


class ExtractionTests(unittest.TestCase):
    """Exercise extraction into a fresh directory without existing subdirectories."""

    def test_original_resource_bytes_and_names(self):
        """The exporter must create resource directories and preserve source bytes."""
        resources = {
            "songs/songlist": b'{"songs":[{"id":"example","title_localized":{"en":"Example"}}]}\n',
            "songs/packlist": b'{"packs":[{"id":"pack","name_localized":{"en":"Pack"}}]}\n',
            "songs/unlocks": b'{"unlocks":[]}\n',
            "char/characters.json": b'[{"character_id":0}]\n',
        }
        apk = io.BytesIO()
        with zipfile.ZipFile(apk, "w") as archive:
            for name, content in resources.items():
                archive.writestr(f"assets/{name}", content)
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "intl"
            with (
                patch.multiple(
                    update,
                    OUTPUT_PACKLIST_FILE=output / "songs/packlist",
                    OUTPUT_SONGLIST_FILE=output / "songs/songlist",
                    OUTPUT_UNLOCKS_FILE=output / "songs/unlocks",
                    OUTPUT_CHARACTERS_FILE=output / "char/characters.json",
                    OUTPUT_VERSION_FILE=output / "version",
                ),
                patch.object(update, "extract_story_sources_from_apk_zip"),
                patch.object(update, "extract_tl_from_apk_zip"),
            ):
                packs, songs = update.extract_outputs_from_apk(apk, "7.1.0")
            self.assertEqual(packs, {"pack": "Pack"})
            self.assertEqual(songs, {"example": "Example"})
            for name, content in resources.items():
                self.assertEqual((output / name).read_bytes(), content)
            self.assertEqual((output / "version").read_text(), "7.1.0\n")


if __name__ == "__main__":
    unittest.main()
