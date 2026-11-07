# Arcaea Wiki Data

This repository contains the mobile-version Lua data files for Chinese Arcaea Wiki. The data is sourced from the Arcaea game APK and is updated regularly to reflect changes in the game.

It also owns the automated producer for [Arcaea Story History](https://github.com/SkyEye-FAST/arcaea_story).
The history repository stores complete `story/` and `story2/` snapshots, including
images, audio, and layout resources. Its commits record content changes using the
game version as the commit message. Automation changes belong in this repository.
The private Arcaea Data package remains independently versioned for application
consumers; Wiki outputs and story assets are not package inputs.

The generated story outputs should be synchronized to [Module:Story/data/mobile](https://wiki.arcaea.cn/Module:Story/data/mobile) (including [/zh-hans](https://wiki.arcaea.cn/Module:Story/data/mobile/zh-hans), [/zh-hant](https://wiki.arcaea.cn/Module:Story/data/mobile/zh-hant), [/en](https://wiki.arcaea.cn/Module:Story/data/mobile/en), [/ja](https://wiki.arcaea.cn/Module:Story/data/mobile/ja), [/ko](https://wiki.arcaea.cn/Module:Story/data/mobile/ko)). Nintendo Switch story data is maintained separately and is not generated or uploaded by this repository.

Other generated outputs are synchronized to [Template:Translation.json](https://wiki.arcaea.cn/Template:Translation.json), [Template:Version](https://wiki.arcaea.cn/Template:Version), [Template:Songlist.json](https://wiki.arcaea.cn/Template:Songlist.json), [Template:Packlist.json](https://wiki.arcaea.cn/Template:Packlist.json), [Template:Unlocks.json](https://wiki.arcaea.cn/Template:Unlocks.json), [Template:Characters.json](https://wiki.arcaea.cn/Template:Characters.json), and [Module:Arcaea/Index.json](https://wiki.arcaea.cn/Module:Arcaea/Index.json) after each update.

To generate outputs directly from a local APK, run:

```bash
uv run update.py --apk /path/to/arcaea_7.0.0c.apk
```

The version is derived from the APK filename. Use `--version` when the filename does not contain the game version.

The update workflow also uploads new song jackets, pack covers, partner
illustrations/icons, and gameplay backgrounds from the same APK. It uses existing
Wiki filenames (`Songs_`, `Pack_`, `Partner_`, `Background_`), prefers full-size
artwork, and excludes thumbnails, multiplayer crops, and background effect layers.
Before generation, the uploader compares the APK with the checkout's existing
`output/songlist`, `output/packlist`, and `output/characters.json`. Resources in
those lists are assumed already uploaded: historical gaps are not scanned or
backfilled. Only new songs/packs/partners, added jacket overrides/awakenings, and
newly referenced backgrounds become candidates. Wiki existence checks apply only
to these additions, preserving existing files, redirects, and shared files.
Upload failures fail the step after the remaining images have been attempted;
generated lists are committed only after successful artwork publication, keeping
the old baseline available for retries.
The upload summary is `{{Information Arcaea}}`; file descriptions contain that
template and `{{Non-free fair use in}}`, equivalent to choosing **通用** in the
upload form's license selector. The API does not have a separate license field.

Before updating the output lists, preview and upload additions:

```bash
uv run sync_images.py --apk /path/to/arcaea_7.1.0c.apk --dry-run
uv run sync_images.py --apk /path/to/arcaea_7.1.0c.apk
```

Use `--baseline /path/to/old-output` if the working output lists were already updated.

The uploader uses the same Wiki credentials as `sync_wiki.py` and requires upload
and bot rights. Image binaries are temporary and are never committed to Git.

To export the complete story snapshot into a separate Git checkout, run:

```bash
uv run story_history.py --apk /path/to/arcaea_7.0.0c.apk --repository /path/to/story-history
```

The exporter validates the snapshot before replacing the managed directories,
including files removed upstream. Files outside `story/` and `story2/` are preserved.

## Automation

Tencent Cloud SCF dispatches `.github/workflows/update.yml` on `main`, without
inputs. There is no GitHub cron and no workflow in the history repository.
Deploy `ops/scf_dispatch.py` as `index.py` with entry point `index.main_handler`.
Set its `GITHUB_TOKEN` environment variable to the existing dispatch credential.
Keep only the `arcaea_wiki_data` timer at 07:54 Asia/Shanghai
(`0 54 7 * * * *`). Remove the two former story timers after validating the
unified publisher; the release listener replaces those repeated dispatches.
Dispatch failures propagate to SCF; an accepted dispatch does not imply a successful run.

The workflow serializes updates and acquires one validated APK for both publishers.
During 07:50–08:30 Asia/Shanghai it polls every ten seconds for a new version and
attempts an early Wiki version notice before downloading. At the deadline, or
outside that window, it processes the current APK. Unchanged versions are downloaded
too, so retries and same-version content revisions are not skipped.

Story publication and Wiki generation/synchronization run independently after
acquisition. Generated Wiki files are committed even if Wiki synchronization fails.
Failures remain visible in the workflow result; rerun the workflow to retry. The
early version notice does not modify the generated `output/version` file.

Required repository secrets:

- `PYWIKIBOT_PASSWORD_FILE_CONTENT`: the existing Wiki bot password configuration.
- `STORY_DEPLOY_KEY`: a dedicated SSH private deploy key whose public key has write
  access to `SkyEye-FAST/arcaea_story` only. The workflow's own token writes Wiki outputs.

APKs and acquisition metadata stay in ignored `.pipeline/` storage, never Git.
Run `uv run ruff check .` and `uv run python -m unittest discover -s tests` before
publishing pipeline changes. Test exports against the same APK before switching
the SCF target; remove the previous history dispatch after the new publisher works.
