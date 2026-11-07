"""Exercise artwork selection and safe, repeatable uploads."""

import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

import sync_images as images


class ArtworkTests(unittest.TestCase):
    """Check filenames and publication boundaries."""

    def archive(self):
        """Build a small representative game archive."""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("assets/songs/songlist", json.dumps({"songs": [
                {"id": "example", "bg": "example", "difficulties": [
                    {"ratingClass": 3, "jacketOverride": True},
                ]},
            ]}))
            archive.writestr("assets/songs/packlist", json.dumps({"packs": [
                {"id": "example"}, {"id": "example_append_1"},
            ]}))
            archive.writestr("assets/char/characters.json", json.dumps([
                {"character_id": number, "uncap_cores": [{"amount": 1}]}
                for number in [0, 1, 11, 71]
            ]))
            for name in [
                "songs/dl_example/base.jpg", "songs/dl_example/1080_base.jpg",
                "songs/dl_example/1080_3.jpg", "songs/dl_example/1080_base_256.jpg",
                "songs/dl_example/1080_base_night.jpg", "songs/dl_example/1080_base_ja.jpg",
                "songs/pack/1080_select_example.png",
                "songs/pack/1080/select_example_append_1.png",
                "songs/pack/1080/select_example_append_1-pressed.png",
                "char/1080/0.png", "char/1080/0o.png", "char/1080/11u.png",
                "char/11u_icon.png", "char/71l_icon.png", "char/0o_icon.png",
                "char/1l_icon.png", "char/1080/1l.png",
                "char/11_mp.png", "img/bg/1080/example.jpg",
                "img/bg/1080/example_clear.png", "img/bg/1080/debris.png",
            ]:
                archive.writestr("assets/" + name, b"artwork")
        return zipfile.ZipFile(buffer)

    def test_selection(self):
        """Prefer full-size assets, map variants, and exclude UI resources."""
        with self.archive() as archive:
            result = {item.title: item.member for item in images.collect_artwork(archive)}
        self.assertEqual(set(result), {
            "Songs_example.jpg", "Songs_example_byd.jpg", "Pack_example.png",
            "Songs_example_night.jpg", "Songs_example_ja.jpg",
            "Pack_example_append_1.png", "Partner_0_new.png", "Partner_0_old.png",
            "Partner_11_awaken.png", "Partner_11_awaken_icon.png",
            "Partner_71_lost_icon.png", "Partner_0_icon_old.png", "Background_example.jpg",
            "Partner_1_lost.png",
        })
        self.assertEqual(result["Songs_example.jpg"], "assets/songs/dl_example/1080_base.jpg")

    def test_existing_lists_assume_uploaded(self):
        """Unchanged lists exclude all artwork, including historical missing files."""
        with self.archive() as archive, tempfile.TemporaryDirectory() as directory:
            baseline = Path(directory)
            for filename, member in [
                ("songlist", "assets/songs/songlist"),
                ("packlist", "assets/songs/packlist"),
                ("characters.json", "assets/char/characters.json"),
            ]:
                (baseline / filename).write_bytes(archive.read(member))
            self.assertEqual(images.new_artwork(archive, baseline), [])
            # Existing entities can gain a jacket override and an awakening.
            songs = json.loads((baseline / "songlist").read_bytes())
            songs["songs"][0]["difficulties"] = []
            (baseline / "songlist").write_text(json.dumps(songs))
            characters = json.loads((baseline / "characters.json").read_bytes())
            characters[2]["uncap_cores"] = []
            (baseline / "characters.json").write_text(json.dumps(characters))
            self.assertEqual({item.title for item in images.new_artwork(archive, baseline)}, {
                "Songs_example_byd.jpg", "Partner_11_awaken.png", "Partner_11_awaken_icon.png",
            })

    def test_new_entities_and_background(self):
        """Select additions from the APK rather than scanning Wiki gaps."""
        with self.archive() as archive, tempfile.TemporaryDirectory() as directory:
            baseline = Path(directory)
            for filename, value in [
                ("songlist", {"songs": []}), ("packlist", {"packs": []}),
                ("characters.json", []),
            ]:
                (baseline / filename).write_text(json.dumps(value))
            self.assertEqual(images.new_artwork(archive, baseline), images.collect_artwork(archive))

    def test_missing_normalized_redirect_shared(self):
        """Existing files, redirects and shared binaries must not be uploaded."""
        site = Mock()
        site.simple_request.return_value.submit.return_value = {"query": {
            "normalized": [{"from": "File:Songs_example.jpg", "to": "File:Songs example.jpg"}],
            "redirects": [{"from": "File:redirect.png", "to": "File:target.png"}],
            "pages": [
                {"title": "File:Songs example.jpg", "imageinfo": [{}]},
                {"title": "File:target.png", "imageinfo": [{}]},
                {"title": "File:shared.png", "missing": True, "imageinfo": [{}]},
                {"title": "File:new.png", "missing": True},
            ],
        }}
        artwork = [images.Artwork("member", title, "jacket") for title in [
            "Songs_example.jpg", "redirect.png", "shared.png", "new.png",
        ]]
        self.assertEqual(images.missing_artwork(site, artwork), [artwork[-1]])

    def test_upload_metadata_and_failure(self):
        """Supply the selected license and surface failures after continuing."""
        site = Mock()
        site.upload.side_effect = [False, True]
        pending = [images.Artwork("assets/char/11u_icon.png", f"{n}.png", "icon")
                   for n in range(2)]
        with self.archive() as archive, patch.object(images, "missing_artwork",
                                                     side_effect=lambda site, items: items), \
                patch.object(images.pywikibot, "FilePage"):
            with self.assertRaisesRegex(RuntimeError, "Failed to upload 1 images"):
                images.upload_artwork(site, archive, pending)
        self.assertEqual(site.upload.call_count, 2)
        for call in site.upload.call_args_list:
            self.assertEqual(call.kwargs["comment"], "{{Information Arcaea}}")
            self.assertIn("{{Non-free fair use in}}", call.kwargs["text"])
            self.assertFalse(call.kwargs["ignore_warnings"])


if __name__ == "__main__":
    unittest.main()
