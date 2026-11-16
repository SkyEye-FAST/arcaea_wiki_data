#!/system/bin/sh
# Export active CN resources on Android without a host-side ADB connection.
set -eu
umask 077

fail() {
    printf '%s\n' "$*" >&2
    exit 1
}

[ "$#" -le 1 ] || fail "Usage: sh extract-cn.sh [OUTPUT_DIRECTORY]"
[ "$(id -u)" = 0 ] || fail "Run this script as root (su -c)."
shared_root=/mnt/shared/MuMuShared
if [ "$#" -eq 0 ]; then
    output=$shared_root/Arcaea/cn-$(date +%Y%m%d-%H%M%S)
else
    output=${1%/}
fi
case "$output" in
    "$shared_root"/*)
        grep -q " $shared_root " /proc/mounts \
            || fail "MuMu shared folder is not mounted; provide a shared storage destination."
        ;;
    /sdcard/*|/storage/emulated/0/*) ;;
    *) fail "Choose a destination in shared Android storage." ;;
esac
case "$output" in
    */../*|*/..|*/./*|*/.) fail "Use a path without dot components." ;;
esac
[ ! -e "$output" ] || fail "Destination already exists; choose a new export directory."

package=com.xd.arcaea.tapcn
active_root=/data/data/$package/files/cb/active
resources='songs/songlist songs/packlist songs/unlocks tl/zh-Hans_merged_automatic.mo'

version_fields() {
    info=$(dumpsys package "$package")
    version_name=$(printf '%s\n' "$info" | sed -n 's/^[[:space:]]*versionName=\([^[:space:]]*\).*/\1/p' | head -n 1)
    version_code=$(printf '%s\n' "$info" | sed -n 's/^[[:space:]]*versionCode=\([0-9][0-9]*\).*/\1/p' | head -n 1)
    case "$version_name" in
        ''|*[!A-Za-z0-9.+_-]*) fail "CN package versionName is missing or invalid." ;;
    esac
    case "$version_code" in
        ''|*[!0-9]*) fail "CN package versionCode is missing or invalid." ;;
    esac
    printf '%s\n%s\n' "$version_name" "$version_code"
}

fingerprint() {
    for resource in $resources; do
        checksum=$(sha256sum "$1/$resource") || return 1
        printf '%s %s\n' "${checksum%% *}" "$resource"
    done
}

version_before=$(version_fields)
before=$(fingerprint "$active_root")
parent=$(dirname "$output")
mkdir -p "$parent"
staging=$(mktemp -d "$parent/.arcaea-cn.XXXXXX")
cleanup() {
    case "$staging" in
        "$parent"/.arcaea-cn.*) rm -rf -- "$staging" ;;
    esac
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

for resource in $resources; do
    mkdir -p "$staging/$(dirname "$resource")"
    cat "$active_root/$resource" > "$staging/$resource"
done
after=$(fingerprint "$active_root")
copied=$(fingerprint "$staging")
version_after=$(version_fields)
[ "$before" = "$after" ] && [ "$before" = "$copied" ] && [ "$version_before" = "$version_after" ] \
    || fail "CN resources changed during export; finish the game update and retry."

version_name=$(printf '%s\n' "$version_before" | head -n 1)
version_code=$(printf '%s\n' "$version_before" | tail -n 1)
{
    printf '{\n  "package": "%s",\n  "active_root": "%s",\n' "$package" "$active_root"
    printf '  "versionName": "%s",\n  "versionCode": "%s",\n  "files": {\n' "$version_name" "$version_code"
    separator=''
    for resource in $resources; do
        checksum=$(sha256sum "$staging/$resource")
        printf '%s    "%s": {"sha256": "%s"}' "$separator" "$resource" "${checksum%% *}"
        separator=',
'
    done
    printf '\n  }\n}\n'
} > "$staging/source.json"

# Both directories are siblings on shared storage; only a complete export is published.
mv "$staging" "$output"
staging=''
printf 'CN %s (%s) exported to %s\n' "$version_name" "$version_code" "$output"
