# Arcaea Wiki Data

Extract mobile client data for the Chinese Arcaea Wiki and publish complete
snapshots to [Arcaea Story History](https://github.com/SkyEye-FAST/arcaea_story).
International APK automation and local CN extraction share publication utilities
but use separate output directories and Wiki page mappings.

## Repository layout

```text
arcaea_wiki_data/          Python modules; run with python -m arcaea_wiki_data.<module>
  paths.py                Shared repository and region paths
  update.py               International APK data and story export
  listen_update.py        Release acquisition and early version notice
  story_history.py        Complete story snapshot export
  sync_wiki.py            Wiki page publication
  sync_images.py          New artwork publication
  sync_cn.py              Local rooted CN client extraction and publication
  cn_overlay.py           CN differences against the international baseline
data/manual.json          Maintained story metadata overrides
docs/                     English usage documentation
ops/                      Cloud function dispatcher
tests/                    Offline validation
output/
  intl/                   International outputs
    songs/                Original songlist, packlist, unlocks
    char/characters.json  Original partner data
    tl/                   Original MO catalogs and decompiled PO files
    tl.json               Generated translation dictionary
    version               Generated version state
    arcaea_index.json     Generated song index
    arcaea_story_*.lua    Generated story modules
  cn/                     Local CN snapshot; original resource names and paths
    songs/                Original songlist, packlist, unlocks
    tl/                   Original merged Simplified Chinese MO catalog
    tl.json               Generated CN translation dictionary
    CN_Overlay.json       Generated song and pack differences for the Wiki
    manifest.json         CN source metadata and integrity hashes
user-config.py            Pywikibot site configuration
user-password.cfg         Local credentials, ignored by Git
.pipeline/                Temporary APK downloads, ignored by Git
.local/                   Machine-specific notes and settings, ignored by Git
```

Raw resources keep their client filenames and relative paths; only CN Wiki
destination titles use `_CN` suffixes.

## Local commands

Install the locked environment and export an international APK:

```bash
uv sync --frozen
uv run python -m arcaea_wiki_data.update --apk /path/to/arcaea_7.1.0c.apk
```

The version is derived from the APK filename unless `--version` is passed.

For rooted Android CN extraction and publication, see [CN sync](docs/cn-sync.md).

## Wiki publication

International outputs map to
[Module:Story/data/mobile](https://wiki.arcaea.cn/Module:Story/data/mobile) (and
its `/zh-hans`, `/zh-hant`, `/en`, `/ja`, `/ko` subpages),
`Template:Translation.json`, `Template:Version`, `Template:Songlist.json`,
`Template:Packlist.json`, `Template:Unlocks.json`, `Template:Characters.json`, and
`Module:Arcaea/Index.json`. Nintendo Switch story data is maintained separately.

```bash
uv run python -m arcaea_wiki_data.sync_wiki --dry-run
uv run python -m arcaea_wiki_data.sync_wiki
```

Before replacing `output/intl/`, preview and upload new artwork:

```bash
uv run python -m arcaea_wiki_data.sync_images --apk /path/to/arcaea_7.1.0c.apk --dry-run
uv run python -m arcaea_wiki_data.sync_images --apk /path/to/arcaea_7.1.0c.apk
```

The uploader compares the APK against `output/intl/` (`--baseline` if lists
were already updated; must contain `songs/` and `char/`) and uploads only new
songs, packs, partners, jacket overrides, awakenings, and newly referenced
backgrounds. Existing files, redirects, and shared files are skipped, and
historical gaps are not backfilled.

Artwork uses `Songs_`, `Pack_`, `Partner_`, and `Background_` filenames with
full-size assets (excluding thumbnails, multiplayer crops, and background effect
layers), using `{{Information Arcaea}}` and `{{Non-free fair use in}}`
(upload and bot rights required). Any failed candidate fails the step after
attempting the rest so the previous baseline remains intact.

## Story history

```bash
uv run python -m arcaea_wiki_data.story_history --apk /path/to/arcaea_7.1.0c.apk --repository /path/to/story-history
```

Validates complete `story/` and `story2/` snapshots (images, audio, and layout
resources) before replacing managed directories, removing deleted upstream files
while preserving unrelated ones. History commits use the game version as their
message.

## Automation

Tencent Cloud SCF dispatches `.github/workflows/update.yml` on `main` without
inputs (no GitHub cron, no workflow in `arcaea_story`). Deploy
`ops/scf_dispatch.py` as `index.py` (handler `index.main_handler`) with
`GITHUB_TOKEN`, scheduled at 07:54 Asia/Shanghai (`0 54 7 * * * *`).

Each run acquires one validated APK: during 07:50-08:30 Asia/Shanghai it polls
every 10 seconds and attempts an early version notice before downloading a new
version; at the deadline or outside that window, it processes the current APK.
Unchanged versions are also downloaded for story retries and same-version
revisions.

Story export runs on every acquisition. Artwork upload, `output/intl/` and
regenerated `output/cn/CN_Overlay.json` commits, and Wiki page sync run when
the acquired version differs from `output/intl/version` (or when
`force_wiki_sync` is enabled manually). Generated data is committed even if page
sync fails; re-run with `force_wiki_sync` to retry.

Required repository secrets:

- `PYWIKIBOT_PASSWORD_FILE_CONTENT`: Wiki bot password configuration.
- `STORY_DEPLOY_KEY`: SSH deploy key with write access to
  `SkyEye-FAST/arcaea_story`.

## Validation

```bash
uv run ruff check .
uv run python -m unittest discover -s tests
```

Keep documentation in English and use portable examples; machine-specific paths,
device names, and connection addresses belong in `.local/`.
