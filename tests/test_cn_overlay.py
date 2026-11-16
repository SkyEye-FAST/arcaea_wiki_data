"""Check the generated delta against the existing Lua consumer's merge semantics."""

import copy
import unittest

from arcaea_wiki_data.cn_overlay import list_overlay


def reconstruct(entries: list, overlay: dict) -> list:
    """Apply the consumer's order, shallow nested merge, deletions, and append rules."""
    indexed = {item["id"]: item for item in entries}
    order = overlay.get("order", list(indexed))
    result = []
    for identifier in order:
        item = copy.deepcopy(indexed[identifier])
        patch = overlay.get("patches", {}).get(identifier, {})
        for key in patch.get("_delete", []):
            item.pop(key, None)
        for key, value in patch.items():
            if key == "_delete":
                continue
            if isinstance(value, dict) and isinstance(indexed[identifier].get(key), dict):
                nested = copy.deepcopy(indexed[identifier][key])
                for removed in value.get("_delete", []):
                    nested.pop(removed, None)
                nested.update(
                    {
                        name: copy.deepcopy(entry)
                        for name, entry in value.items()
                        if name != "_delete"
                    }
                )
                item[key] = nested
            else:
                item[key] = copy.deepcopy(value)
        result.append(item)
    return result + copy.deepcopy(overlay.get("added", []))


class OverlayTests(unittest.TestCase):
    """Verify exact CN reconstruction, including structural changes."""

    def check_delta(self, base, target):
        """Compare reconstruction to the complete target, not a patch snapshot."""
        overlay = list_overlay({"songs": base}, {"songs": target}, "songs")
        self.assertEqual(reconstruct(base, overlay), target)
        return overlay

    def test_localized_fields_and_deletions(self):
        """Nested localized removals and deeper values follow the two-level Lua merge."""
        base = [
            {
                "id": "a",
                "banner": "old",
                "side": 0,
                "localized": {"en": "Name", "ja": "Old", "zh": "Old"},
                "extra": {"nested": {"keep": 1, "remove": 2}},
            }
        ]
        target = [
            {
                "id": "a",
                "side": 1,
                "localized": {"en": "Name", "zh": "New"},
                "extra": {"nested": {"keep": 3}},
            }
        ]
        delta = self.check_delta(base, target)
        self.assertEqual(delta["patches"]["a"]["_delete"], ["banner"])
        self.assertEqual(delta["patches"]["a"]["localized"]["_delete"], ["ja"])

    def test_add_remove_reorder_and_empty_lists(self):
        """Preserve target order even when new entries occur between existing IDs."""
        base = [{"id": identifier} for identifier in "abc"]
        for identifiers in ("abc", "bac", "ac", "abcd", "adbc", "dab", "", "d"):
            with self.subTest(identifiers=identifiers):
                self.check_delta(base, [{"id": identifier} for identifier in identifiers])

    def test_arrays_and_empty_maps(self):
        """An empty array must clear the old value instead of being merged as a map."""
        for old, new in [
            ([1, 2], []),
            ([1], [False, True]),
            ([1], {}),
            ({"a": 1}, {}),
            ([1, 2], [3]),
            ({"deep": [1]}, {"deep": []}),
        ]:
            with self.subTest(old=old, new=new):
                self.check_delta([{"id": "a", "value": old}], [{"id": "a", "value": new}])

    def test_boolean_is_not_number(self):
        """Boolean and numeric JSON values must not collapse through Python equality."""
        overlay = self.check_delta([{"id": "a", "value": 1}], [{"id": "a", "value": True}])
        self.assertIs(overlay["patches"]["a"]["value"], True)

    def test_duplicate_and_invalid_ids(self):
        """Ambiguous entries must fail instead of being silently overwritten."""
        for entries in ([{"id": "a"}, {"id": "a"}], [{"id": ""}], [{}]):
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                list_overlay({"songs": []}, {"songs": entries}, "songs")


if __name__ == "__main__":
    unittest.main()
