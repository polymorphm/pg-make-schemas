project = "pg-make-schemas"
author = "Andrei Antonov"
copyright = "2017, 2018, 2019, 2024, 2026, Andrei Antonov"

extensions = [
    "sphinx.ext.githubpages",
]

source_suffix = {
    ".rst": "restructuredtext",
}

master_doc = "index"

exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
]

nitpicky = True

html_theme = "furo"
html_title = "pg-make-schemas documentation"
html_short_title = "pg-make-schemas"

html_static_path = []
