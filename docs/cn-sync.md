# CN extraction and Wiki sync

Extract CN client resources from a rooted Android device or emulator and
publish them to the Wiki in two separate steps.

## Requirements

- CN client `com.xd.arcaea.tapcn` with completed downloads under
  `/data/data/com.xd.arcaea.tapcn/files/cb/active/`.
- Root access through `su -c`.
- Python environment initialized with `uv sync --frozen`, plus current
  `output/intl/` tables for overlay generation.

## Option A: direct ADB extraction

Read the serial or current `host:port` from `adb devices -l` (emulator ports may
change on restart):

```bash
uv run python -m arcaea_wiki_data.sync_cn extract --serial DEVICE_SERIAL
```

Pass `--adb /path/to/adb` if `adb` is not on `PATH` (Windows `adb.exe` works
from WSL).

## Option B: export inside Android

Copy `tools/extract-cn.sh` to shared Android storage. On MuMu, run it from a
root terminal or automation app without an output argument:

```sh
su -c 'sh /sdcard/Download/extract-cn.sh'
```

The default destination is `/mnt/shared/MuMuShared/Arcaea/cn-YYYYMMDD-HHMMSS`.
It is directly accessible under `Arcaea/` in the configured Windows shared
folder; no separate copy or ADB pull is needed. The script requires the shared
folder to be mounted before writing there.

For another rooted Android environment, pass a fresh directory explicitly:

```sh
su -c 'sh /sdcard/Download/extract-cn.sh /sdcard/Download/cn-export'
```

The script verifies file hashes and package version before and after copying,
writing `source.json` and publishing the directory atomically. Import the
directory from its Windows or WSL shared-folder path (or transfer it first when
using a device without shared folders):

```bash
uv run python -m arcaea_wiki_data.sync_cn import --source /path/to/cn-export
```

## Snapshot layout

Both options write to `output/cn/` by default (`--output` and `--intl` override
the snapshot and baseline directories):

| File relative to `output/cn/` | Purpose or Wiki destination |
| --- | --- |
| `songs/songlist` | Original data; `Template:Songlist_CN.json` |
| `songs/packlist` | Original data; `Template:Packlist_CN.json` |
| `songs/unlocks` | Local comparison only; no CN unlock page |
| `tl/zh-Hans_merged_automatic.mo` | Original merged CN catalog (omitted from `zh-Hans.mo`) |
| `tl.json` | Generated string dictionary; `Template:Translation_CN.json` |
| `CN_Overlay.json` | Generated differences; `Module:Arcaea/CN overlay.json` |
| `manifest.json` | Client metadata, source paths, entry counts, and hashes |

MO binaries are ignored by Git, so re-extract or import the snapshot before
publishing from a new checkout.

## Overlay generation

`CN_Overlay.json` encodes song and pack differences against `output/intl/songs/`
using the Lua consumer's `patches`, `_delete`, `order`, and `added` rules. It is
updated automatically during CN extract/import/upload and by
`arcaea_wiki_data.update` when CN source tables exist, or manually via:

```bash
uv run python -m arcaea_wiki_data.cn_overlay
```

Use `--intl` and `--cn` for custom regional directories.

## Preview and publish

```bash
uv run python -m arcaea_wiki_data.sync_cn upload --dry-run
uv run python -m arcaea_wiki_data.sync_cn upload
```

Use `--input` and `--intl` for non-default directories. A dry run refreshes the
local overlay without logging in or editing the Wiki.

Production uses `user-config.py` with `user-password.cfg` or
`PYWIKIBOT_PASSWORD_FILE_CONTENT`. For another Wiki, pass
`--wiki-url https://wiki.example.org/api.php --user USERNAME` with an account
configured under Pywikibot family/code `cnwiki`.

Equivalent JSON is skipped regardless of whitespace or key order, and missing
pages use the `json` content model. For manual edits, paste `songlist`,
`packlist`, `tl.json`, or `CN_Overlay.json` directly into the target JSON page
rather than using `Special:Upload`.
