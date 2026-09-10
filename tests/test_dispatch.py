"""Verify the cloud function's single target and failure signaling."""

import os
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

from ops.scf_dispatch import DISPATCH_URL, main_handler


class DispatchTests(unittest.TestCase):
    """Validate the request contract without making a network request."""

    def test_dispatches_only_unified_producer(self):
        """Send the existing no-input workflow_dispatch contract."""
        response = MagicMock()
        response.__enter__.return_value.status = 204
        with patch.dict(os.environ, {"GITHUB_TOKEN": "test-token"}):
            with patch("urllib.request.urlopen", return_value=response) as send:
                result = main_handler({}, None)
        request = send.call_args.args[0]
        self.assertEqual(request.full_url, DISPATCH_URL)
        self.assertEqual(request.data, b'{"ref": "main"}')
        self.assertTrue(result["dispatched"])

    def test_failed_dispatch_is_not_reported_as_success(self):
        """Leave failures visible to SCF instead of returning an error string."""
        with patch.dict(os.environ, {"GITHUB_TOKEN": "test-token"}):
            with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("unavailable")):
                with self.assertRaises(urllib.error.URLError):
                    main_handler({}, None)
