"""
Macro generator for image galleries in MkDocs.

This module provides a macro that automatically renders all images from a folder
as a responsive grid. It's used by mkdocs-macros-plugin.

Usage in markdown:
    {{ image_folder("img", cols=4) }}
    {{ image_folder("assets/diagrams", cols=3) }}
"""

import html

from mkdocs.utils import normalize_url

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}


def define_env(env):
    """
    Define the image_folder macro for mkdocs-macros-plugin.

    Args:
        env: The mkdocs-macros environment object.
    """

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
