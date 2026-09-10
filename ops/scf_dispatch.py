"""Tencent Cloud SCF entry point for the unified Arcaea producer."""

import json
import os
import urllib.request

DISPATCH_URL = "https://api.github.com/repos/SkyEye-FAST/arcaea_wiki_data/actions/workflows/update.yml/dispatches"


def main_handler(event, context):
    """Dispatch the producer and propagate failures to SCF's retry mechanism."""
    request = urllib.request.Request(
        DISPATCH_URL,
        data=json.dumps({"ref": "main"}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "Arcaea-SCF-Dispatcher",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 204:
            raise RuntimeError(f"Unexpected GitHub dispatch status: {response.status}")
    print("Accepted dispatch for arcaea_wiki_data/update.yml on main")
    return {"dispatched": True, "repository": "arcaea_wiki_data", "ref": "main"}
