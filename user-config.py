"""Pywikibot local configuration for wiki.arcaea.cn."""

import os

family = "arcaea"
mylang = "arcaea"

family_files = {
    "arcaea": "https://wiki.arcaea.cn/api.php",
}

login_user = (
    os.environ.get("PYWIKIBOT_LOGIN_USER") or os.environ.get("PYWIKIBOT_USERNAME") or "Masertwer"
)
usernames["arcaea"]["arcaea"] = login_user  # type: ignore  # noqa: F821

password_file = "user-password.cfg"

put_throttle = 0
maxlag = 5

console_encoding = "utf-8"
