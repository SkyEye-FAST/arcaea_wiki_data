"""Pywikibot local configuration for wiki.arcaea.cn."""

family = "arcaea"
mylang = "arcaea"

family_files = {
    "arcaea": "https://wiki.arcaea.cn/api.php",
}

usernames["arcaea"]["arcaea"] = "Masertwer"  # type: ignore  # noqa: F821

password_file = "user-password.cfg"

put_throttle = 0
maxlag = 5

console_encoding = "utf-8"
