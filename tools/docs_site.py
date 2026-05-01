#!/usr/bin/env python3

import html
import pathlib
import re
import sys

import yaml


def load_manifest(path):
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    if not isinstance(data, dict):
        raise SystemExit("manifest must be a mapping")

    return data


def check_path(path):
    if not isinstance(path, str) or not path:
        raise SystemExit("entry path must be a non-empty string")
    if path.startswith("/") or path.endswith("/"):
        raise SystemExit(f"invalid output path: {path!r}")
    if path == "." or ".." in pathlib.PurePosixPath(path).parts:
        raise SystemExit(f"invalid output path: {path!r}")


def entry_from_mapping(key, data):
    if not isinstance(data, dict):
        raise SystemExit(f"{key} entry must be a mapping")

    title = data.get("title")
    ref = data.get("ref")
    path = data.get("path")

    if not isinstance(title, str) or not title:
        raise SystemExit(f"{key}.title must be a non-empty string")
    if not isinstance(ref, str) or not ref:
        raise SystemExit(f"{key}.ref must be a non-empty string")

    check_path(path)

    return {
        "key": key,
        "title": title,
        "ref": ref,
        "path": path,
    }


def entries(data):
    result = []

    if data.get("developing") is not None:
        result.append(entry_from_mapping("developing", data["developing"]))

    if data.get("current") is not None:
        result.append(entry_from_mapping("current", data["current"]))

    versions = data.get("versions", [])
    if versions is None:
        versions = []
    if not isinstance(versions, list):
        raise SystemExit("versions must be a list")

    for index, item in enumerate(versions):
        entry = entry_from_mapping(f"version-{index + 1}", item)
        entry["key"] = "version-" + slugify(entry["title"])
        result.append(entry)

    return result


def validate(data):
    site = data.get("site", {})
    if not isinstance(site, dict):
        raise SystemExit("site must be a mapping")

    title = site.get("title")
    if not isinstance(title, str) or not title:
        raise SystemExit("site.title must be a non-empty string")

    seen_keys = set()
    seen_paths = set()
    for entry in entries(data):
        if entry["key"] in seen_keys:
            raise SystemExit(f"duplicate entry key: {entry['key']}")
        if entry["path"] in seen_paths:
            raise SystemExit(f"duplicate output path: {entry['path']}")
        seen_keys.add(entry["key"])
        seen_paths.add(entry["path"])


def slugify(text):
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", text.strip()).strip("-")
    return slug or "version"


def command_entries(manifest):
    data = load_manifest(manifest)
    validate(data)
    for entry in entries(data):
        print("\t".join([entry["key"], entry["title"], entry["ref"], entry["path"]]))


def command_validate(manifest):
    data = load_manifest(manifest)
    validate(data)


def find_entry(data, key):
    for entry in entries(data):
        if entry["key"] == key:
            return entry
    raise SystemExit(f"unknown entry key: {key}")


def link_prefix(output_path):
    depth = len(pathlib.PurePosixPath(output_path).parts)
    return "../" * depth


def command_prepare_build(manifest, key, docs_dir):
    data = load_manifest(manifest)
    validate(data)
    current = find_entry(data, key)
    all_entries = entries(data)

    docs_path = pathlib.Path(docs_dir)
    templates_dir = docs_path / "_templates"
    static_dir = docs_path / "_static"
    templates_dir.mkdir(exist_ok=True)
    static_dir.mkdir(exist_ok=True)

    prefix = link_prefix(current["path"])
    links = [{"title": item["title"], "url": prefix + item["path"] + "/"} for item in all_entries]
    root_url = prefix + "index.html"

    (templates_dir / "layout.html").write_text(
        """{% extends "!layout.html" %}
{% block body %}
<div class="version-switcher">
  <strong>{{ docs_site_current_title }}</strong>
  <a href="{{ docs_site_root_url }}">All versions</a>
  {% for item in docs_site_versions %}
  <a href="{{ item.url }}">{{ item.title }}</a>
  {% endfor %}
</div>
{{ super() }}
{% endblock %}
""",
        encoding="utf-8",
    )

    (static_dir / "version-switcher.css").write_text(
        """.version-switcher {
  border-bottom: 1px solid var(--color-background-border);
  font-size: 0.875rem;
  margin: 0 0 1.25rem;
  padding: 0.75rem 0;
}
.version-switcher strong {
  margin-right: 1rem;
}
.version-switcher a {
  margin-right: 1rem;
}
""",
        encoding="utf-8",
    )

    with open(docs_path / "conf.py", "a", encoding="utf-8") as fh:
        fh.write(
            "\n"
            "# Added by docs-publish build.\n"
            "templates_path = list(globals().get('templates_path', []))\n"
            "if '_templates' not in templates_path:\n"
            "    templates_path.append('_templates')\n"
            "html_static_path = list(globals().get('html_static_path', []))\n"
            "if '_static' not in html_static_path:\n"
            "    html_static_path.append('_static')\n"
            "html_css_files = list(globals().get('html_css_files', []))\n"
            "if 'version-switcher.css' not in html_css_files:\n"
            "    html_css_files.append('version-switcher.css')\n"
            "html_context = {\n"
            f"    'docs_site_current_title': {current['title']!r},\n"
            f"    'docs_site_root_url': {root_url!r},\n"
            f"    'docs_site_versions': {links!r},\n"
            "}\n"
        )


def command_render_index(manifest, site_dir):
    data = load_manifest(manifest)
    validate(data)
    site = data["site"]
    all_entries = entries(data)

    site_path = pathlib.Path(site_dir)
    site_path.mkdir(parents=True, exist_ok=True)

    links = "\n".join(
        f'        <li><a href="{html.escape(entry["path"], quote=True)}/">'
        f'{html.escape(entry["title"])}</a>'
        f'<span>{html.escape(entry["ref"])}</span></li>'
        for entry in all_entries
    )

    description = site.get("description", "")
    page = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html.escape(site["title"])}</title>
    <style>
      body {{
        color: #1f2937;
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        line-height: 1.5;
        margin: 0;
      }}
      main {{
        margin: 0 auto;
        max-width: 52rem;
        padding: 4rem 1.5rem;
      }}
      h1 {{
        font-size: 2rem;
        font-weight: 650;
        letter-spacing: 0;
        margin: 0 0 0.75rem;
      }}
      p {{
        color: #4b5563;
        margin: 0 0 2rem;
      }}
      ul {{
        border-top: 1px solid #d1d5db;
        list-style: none;
        margin: 0;
        padding: 0;
      }}
      li {{
        align-items: baseline;
        border-bottom: 1px solid #d1d5db;
        display: flex;
        gap: 1rem;
        justify-content: space-between;
        padding: 1rem 0;
      }}
      a {{
        color: #005ea8;
        font-weight: 650;
        text-decoration: none;
      }}
      a:hover {{
        text-decoration: underline;
      }}
      span {{
        color: #6b7280;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        font-size: 0.875rem;
      }}
    </style>
  </head>
  <body>
    <main>
      <h1>{html.escape(site["title"])}</h1>
      <p>{html.escape(description)}</p>
      <ul>
{links}
      </ul>
    </main>
  </body>
</html>
"""
    (site_path / "index.html").write_text(page, encoding="utf-8")
    (site_path / ".nojekyll").write_text("", encoding="utf-8")


def main(argv):
    if len(argv) < 3:
        raise SystemExit(
            "usage: docs_site.py validate MANIFEST | entries MANIFEST | "
            "prepare-build MANIFEST KEY DOCS_DIR | render-index MANIFEST SITE_DIR"
        )

    command = argv[1]
    if command == "validate" and len(argv) == 3:
        command_validate(argv[2])
    elif command == "entries" and len(argv) == 3:
        command_entries(argv[2])
    elif command == "prepare-build" and len(argv) == 5:
        command_prepare_build(argv[2], argv[3], argv[4])
    elif command == "render-index" and len(argv) == 4:
        command_render_index(argv[2], argv[3])
    else:
        raise SystemExit(f"invalid arguments for {command!r}")


if __name__ == "__main__":
    main(sys.argv)
