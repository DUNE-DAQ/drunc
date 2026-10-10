"""
Dynamically generate the "Graph > By source directory" pages and nav.

Scans docs/class_diagrams/src/<name>/ for per-directory UML diagrams (produced
by daqpytools-generate-uml, one output folder per drunc source subpackage) and
emits a Classes/Packages page + nav file for each one it finds. Nothing here is
hand-authored, so adding or removing a src/ subpackage just requires
regenerating the diagrams; the docs pick it up on the next build.
"""

from pathlib import Path

import mkdocs_gen_files

SRC_DIAGRAMS_ROOT = "class_diagrams/src"
LEGEND_NAME = "directory_color_legend.svg"

docs_dir = Path(__file__).resolve().parent.parent
src_diagrams_dir = docs_dir / SRC_DIAGRAMS_ROOT

subdir_names = (
    sorted(p.name for p in src_diagrams_dir.iterdir() if p.is_dir())
    if src_diagrams_dir.is_dir()
    else []
)


def emit_diagram_page(virtual_path, title, svg_rel_path, legend_rel_path):
    """Write a virtual page embedding a single svg_viewer diagram."""
    with mkdocs_gen_files.open(virtual_path, "w") as f:
        legend_kw = f', legend="{legend_rel_path}"' if legend_rel_path else ""
        f.write("---\nhide:\n  - toc\n---\n\n")
        f.write(f"# {title}\n\n")
        f.write(f'{{{{ svg_viewer("{svg_rel_path}"{legend_kw}) }}}}\n')


for name in subdir_names:
    base = f"{SRC_DIAGRAMS_ROOT}/{name}"
    legend_rel_path = base + "/" + LEGEND_NAME
    has_legend = (src_diagrams_dir / name / LEGEND_NAME).is_file()

    emit_diagram_page(
        f"graph/src/{name}/classes.md",
        f"{name} \u2014 Classes",
        f"{base}/classes_styled.svg",
        legend_rel_path if has_legend else None,
    )
    emit_diagram_page(
        f"graph/src/{name}/packages.md",
        f"{name} \u2014 Packages",
        f"{base}/packages_styled.svg",
        legend_rel_path if has_legend else None,
    )

    with mkdocs_gen_files.open(f"graph/src/{name}/README.md", "w") as f:
        f.write(f"# {name}\n\n* [Classes](classes.md)\n* [Packages](packages.md)\n")

with mkdocs_gen_files.open("graph/src/README.md", "w") as f:
    f.write("# By source directory\n\n")
    for name in subdir_names:
        f.write(f"* [{name}]({name}/)\n")
