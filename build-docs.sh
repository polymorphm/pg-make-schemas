#!/usr/bin/env bash

set -euo pipefail

cd -- "${0%/*}"

if command -v python3 >/dev/null 2>&1; then
    PYTHON="${PYTHON:-python3}"
else
    PYTHON="${PYTHON:-python}"
fi

tmp_dir="${DOCS_TMP_DIR:-.tmp}"
venv_dir="$tmp_dir/venv"
build_dir="$tmp_dir/build"
site_dir="$tmp_dir/site"
manifest="docs-site.yaml"

resolve_git_ref() {
    local ref="$1"
    local resolved_ref

    if resolved_ref="$(git rev-parse --verify --quiet "$ref^{tree}")"; then
        printf '%s\n' "$resolved_ref"
        return 0
    fi

    if resolved_ref="$(git rev-parse --verify --quiet "origin/$ref^{tree}")"; then
        printf '%s\n' "$resolved_ref"
        return 0
    fi

    printf 'error: cannot resolve documentation ref: %s\n' "$ref" >&2
    printf 'hint: use a local branch/tag/SHA, or fetch origin/%s before building.\n' "$ref" >&2
    return 1
}

mkdir -p "$tmp_dir"

if [ ! -x "$venv_dir/bin/python" ]; then
    "$PYTHON" -m venv "$venv_dir"
fi

"$venv_dir/bin/python" -m pip install -r requirements.txt

rm -rf "$build_dir" "$site_dir"
mkdir -p "$build_dir/refs" "$site_dir"

"$venv_dir/bin/python" tools/docs_site.py validate "$manifest"

while IFS=$'\t' read -r key title ref output_path; do
    ref_dir="$build_dir/refs/$key"
    docs_dir="$ref_dir/docs"
    output_dir="$site_dir/$output_path"

    printf 'Building %s (%s) from %s -> %s\n' "$key" "$title" "$ref" "$output_path"

    mkdir -p "$ref_dir" "$output_dir"
    resolved_ref="$(resolve_git_ref "$ref")"
    git archive "$resolved_ref" README.rst docs | tar -x -C "$ref_dir"

    if [ ! -f "$docs_dir/conf.py" ] || [ ! -f "$docs_dir/index.rst" ]; then
        printf 'error: %s (%s) does not contain Sphinx-ready docs/conf.py and docs/index.rst\n' "$key" "$ref" >&2
        exit 1
    fi

    if [ -f "$docs_dir/requirements.txt" ]; then
        "$venv_dir/bin/python" -m pip install -r "$docs_dir/requirements.txt"
    fi

    "$venv_dir/bin/python" tools/docs_site.py prepare-build "$manifest" "$key" "$docs_dir"
    "$venv_dir/bin/sphinx-build" -W -b html "$docs_dir" "$output_dir"
done < <("$venv_dir/bin/python" tools/docs_site.py entries "$manifest")

"$venv_dir/bin/python" tools/docs_site.py render-index "$manifest" "$site_dir"

printf 'Documentation site generated in %s\n' "$site_dir"
