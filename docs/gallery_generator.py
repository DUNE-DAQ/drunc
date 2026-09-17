"""
Macro generator for image galleries in MkDocs.

This module provides a macro that automatically renders all images from a folder
as a responsive grid. It's used by mkdocs-macros-plugin.

Usage in markdown:
    {{ image_folder("img", cols=4) }}
    {{ image_folder("assets/diagrams", cols=3) }}
"""

import html
from pathlib import Path

from mkdocs.utils import normalize_url

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}


def define_env(env):
    """
    Define the image_folder macro for mkdocs-macros-plugin.

    Args:
        env: The mkdocs-macros environment object.
    """

    @env.macro
    def image_folder(folder, cols=4):
        """
        Render a responsive image grid for a folder under docs/.

        Automatically enumerates all image files in the folder and renders them
        as a grid with captions from filenames. New images are picked up automatically
        on rebuild without requiring markdown changes.

        Usage:
            {{ image_folder("img") }}                    # 4-column grid
            {{ image_folder("assets/diagrams", cols=3) }} # 3-column grid

        Args:
            folder: Path relative to docs/ (e.g., "img" or "assets/generated-images")
            cols:   Number of columns in the grid (default: 4)

        Returns:
            HTML string with grid of images, or error message if folder not found.
        """
        source_docs_dir = Path(__file__).resolve().parent
        target_dir = (source_docs_dir / folder).resolve()

        # Safety check: ensure folder is inside the source docs tree.
        if source_docs_dir not in target_dir.parents and target_dir != source_docs_dir:
            return f"<p><strong>Error:</strong> Image folder path is outside docs/: {html.escape(folder)}</p>"

        if not target_dir.exists() or not target_dir.is_dir():
            return (
                f"<p><strong>Image folder not found:</strong> {html.escape(folder)}</p>"
            )

        # Collect all image files, sorted by name
        files = sorted(
            [
                p
                for p in target_dir.iterdir()
                if p.is_file() and p.suffix.lower() in IMAGE_EXTS
            ],
            key=lambda p: p.name.lower(),
        )

        if not files:
            return f"<p><em>No images found in {html.escape(folder)}</em></p>"

        # Render HTML grid
        col_style = f"repeat({int(cols)}, minmax(0, 1fr))"
        out = [
            f'<div class="image-folder-grid" style="grid-template-columns:{col_style};">'
        ]

        for p in files:
            rel = f"{folder.rstrip('/')}/{p.name}"
            page_rel = f"../{rel}"
            caption = p.stem  # filename without extension

            out.append(
                "<figure>"
                f'<a href="{html.escape(page_rel)}" target="_blank" rel="noopener">'
                f'<img src="{html.escape(page_rel)}" loading="lazy" alt="{html.escape(caption)}">'
                "</a>"
                f"<figcaption>{html.escape(caption)}</figcaption>"
                "</figure>"
            )

        out.append("</div>")
        return "\n".join(out)

    @env.macro
    def svg_viewer(src, legend=None, height="80vh", viewer_id=None):
        """
        Render a pannable/zoomable SVG viewer, with an optional fixed legend overlay.

        The actual SVG content is fetched and injected by docs/javascripts/svg-viewer.js
        at page-load time; this macro only emits the container markup, so adding more
        viewers (e.g. per-folder or split diagrams) is just another macro call.

        Usage:
            {{ svg_viewer("class_diagrams/classes_styled.svg") }}
            {{ svg_viewer("class_diagrams/classes_styled.svg",
                          legend="class_diagrams/directory_color_legend.svg") }}

        Args:
            src:       Path to the SVG, relative to docs/ (e.g. "class_diagrams/classes_styled.svg")
            legend:    Optional path to a legend SVG, relative to docs/, kept fixed on screen
            height:    CSS height of the viewer (default: "80vh")
            viewer_id: Optional explicit element id, auto-generated from src if omitted

        Returns:
            HTML string for the viewer container.
        """
        element_id = viewer_id or f"svg-viewer-{abs(hash(src))}"
        # normalize_url is page-depth aware, unlike a hardcoded '../' prefix.
        data_src = html.escape(normalize_url(src, page=env.page))
        legend_attr = (
            f' data-legend="{html.escape(normalize_url(legend, page=env.page))}"'
            if legend
            else ""
        )

        return (
            f'<div class="svg-viewer" id="{html.escape(element_id)}" '
            f'data-src="{data_src}"{legend_attr} style="height:{html.escape(str(height))};">'
            '<div class="svg-viewer-toolbar">'
            '<button type="button" data-action="zoom-in" title="Zoom in">+</button>'
            '<button type="button" data-action="zoom-out" title="Zoom out">\u2212</button>'
            '<button type="button" data-action="reset" title="Reset view">Reset</button>'
            '<input type="search" class="svg-viewer-search" '
            'placeholder="Search for a class or module…" autocomplete="off">'
            "</div>"
            '<div class="svg-viewer-search-results"></div>'
            '<div class="svg-viewer-canvas"></div>'
            '<div class="svg-viewer-legend"></div>'
            "</div>"
        )
