"""Pywikibot local configuration for wiki.arcaea.cn."""

import os

family = "arcaea"
mylang = "arcaea"

family_files = {
    "arcaea": "https://wiki.arcaea.cn/api.php",
}

if os.environ.get("PYWIKIBOT_USERNAME"):
    usernames["arcaea"]["arcaea"] = os.environ["PYWIKIBOT_USERNAME"]  # type: ignore  # noqa: F821
else:
    usernames["arcaea"]["arcaea"] = "Masertwer"  # type: ignore  # noqa: F821

if os.environ.get("PYWIKIBOT_PASSWORD"):
    password_file = None
else:
    password_file = "user-password.cfg"

put_throttle = 0
maxlag = 5

console_encoding = "utf-8"
