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
            site_rel = f"/{rel}"
            caption = p.stem  # filename without extension

            out.append(
                "<figure>"
                f'<a href="{html.escape(site_rel)}" target="_blank" rel="noopener">'
                f'<img src="{html.escape(site_rel)}" loading="lazy" alt="{html.escape(caption)}">'
                "</a>"
                f"<figcaption>{html.escape(caption)}</figcaption>"
                "</figure>"
            )

        out.append("</div>")
        return "\n".join(out)
