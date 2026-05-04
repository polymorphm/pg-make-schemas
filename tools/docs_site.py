#!/usr/bin/env python3

import html
import pathlib
import re
import sys

import yaml


ASSETS_DIR = pathlib.Path(__file__).with_name("docs_site_assets")
PLACEHOLDER_RE = re.compile(r"{{\s*([A-Za-z0-9_]+)\s*}}")


def read_asset(name):
    return (ASSETS_DIR / name).read_text(encoding="utf-8")


def render_template(template, values):
    placeholders = set(PLACEHOLDER_RE.findall(template))
    missing = placeholders - set(values)
    extra = set(values) - placeholders

    if missing:
        raise SystemExit("missing template values: " + ", ".join(sorted(missing)))
    if extra:
        raise SystemExit("unused template values: " + ", ".join(sorted(extra)))

    def replace(match):
        return values[match.group(1)]

    return PLACEHOLDER_RE.sub(replace, template)


def html_text(value):
    return html.escape(value)


def html_attr(value):
    return html.escape(value, quote=True)


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


def write_asset(target, name):
    target.write_text(read_asset(name), encoding="utf-8")


def render_doc_card(label, description, entry):
    return f"""        <article class="doc-card">
          <p class="card-label">{html_text(label)}</p>
          <h2><a href="{html_attr(entry["path"])}/">{html_text(entry["title"])}</a></h2>
          <p>{html_text(description)}</p>
          <span>{html_text(entry["ref"])}</span>
        </article>"""


def render_version_link(entry):
    return (
        f'          <li><a href="{html_attr(entry["path"])}/">'
        f'{html_text(entry["title"])}</a>'
        f'<span>{html_text(entry["ref"])}</span></li>'
    )


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

    write_asset(templates_dir / "page.html", "page.html")
    write_asset(static_dir / "version-switcher.css", "version-switcher.css")

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
    if not all_entries:
        raise SystemExit("at least one documentation entry is required")

    site_path = pathlib.Path(site_dir)
    site_path.mkdir(parents=True, exist_ok=True)

    developing = data.get("developing")
    current = data.get("current")
    developing_entry = entry_from_mapping("developing", developing) if developing is not None else None
    current_entry = entry_from_mapping("current", current) if current is not None else None

    primary_entries = [
        ("Development docs", "Latest work on the master branch.", developing_entry),
        ("Current stable docs", "Recommended documentation for production users.", current_entry),
    ]
    primary_cards = "\n".join(
        render_doc_card(label, description, entry)
        for label, description, entry in primary_entries
        if entry is not None
    )

    version_links = "\n".join(
        render_version_link(entry) for entry in all_entries
    )

    description = site.get("description", "")
    fallback_entry = all_entries[0]
    developing_url = (developing_entry or fallback_entry)["path"]
    current_url = (current_entry or fallback_entry)["path"]

    page = render_template(
        read_asset("docs-home.html"),
        {
            "site_title_html": html_text(site["title"]),
            "site_description_html": html_text(description),
            "developing_url_attr": html_attr(developing_url),
            "current_url_attr": html_attr(current_url),
            "primary_cards_raw": primary_cards,
            "version_links_raw": version_links,
        },
    )
    (site_path / "index.html").write_text(page, encoding="utf-8")
    write_asset(site_path / "docs-home.css", "docs-home.css")
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
