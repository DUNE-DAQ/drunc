"""
Script to auto-generate MkDocs documentation pages from Python files and docstrings.
"""

import os
from pathlib import Path

import mkdocs_gen_files

# Initialise the navigation object for MkDocs
nav = mkdocs_gen_files.Nav()

root_folder = Path("src/drunc")
excluded_folders = {"__pycache__", "data", "tests"}

for path in root_folder.rglob("*.py"):
    if any(part in excluded_folders for part in path.parts):
        continue

    # Skip if parent folder has no __init__.py. mkdocstrings currently
    # fails for directories without __init__
    if not (path.parent / "__init__.py").exists():
        continue

    # Module and documentation paths relative to root
    module_path = path.relative_to(root_folder).with_suffix("")
    doc_path = path.relative_to(root_folder).with_suffix(".md")

    full_doc_path = Path("Code-Reference", doc_path)

    # Break module path into parts for nav and identifier
    parts = list(module_path.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")
    elif parts[-1] == "__main__":
        continue
    elif parts[-1][0] == "_":
        continue
    if not parts:
        continue

    # Add entry to MkDocs navigation
    nav[tuple(parts)] = str(doc_path)

    os.makedirs(os.path.dirname(full_doc_path), exist_ok=True)

    # Write mkdocstrings to Markdown file
    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        ident = ".".join(["drunc"] + parts)
        print("::: " + ident, file=fd)

    mkdocs_gen_files.set_edit_path(full_doc_path, Path("../") / path)

# Top-level navigation summary
with mkdocs_gen_files.open("Code-Reference/index.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
