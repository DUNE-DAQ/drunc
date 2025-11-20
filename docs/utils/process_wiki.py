"""
Script to covert wiki links to be used in Github Pages Docs. GitHub Wiki uses a Wiki-style link while GitHub Pages uses standard Markdown links.
"""

import os
import re
import sys
from pathlib import Path
from typing import Dict, List

# Configuration
base_url = sys.argv[1].rstrip("/")
wiki_subfolder = sys.argv[2].strip("/")
github_pages_base = base_url + "/"
wiki_link_base = r"https://github.com/DUNE-DAQ/drunc/wiki/"

GH_pages_folder_name = "developer-documentation"


def parse_markdown_nav(file_path: Path) -> List[tuple[list[str], str]]:
    """
    Convert Wiki sidebar navigation into (stack, url).

    Args:
        file_path: The path to the _Sidebar.md file to parse.
    Returns:
        A list of tuples containing (stack, url) representing the navigation structure.
    """
    stack, entries = [], []
    with open(file_path) as f:
        for line in f:
            indent = len(line) - len(line.lstrip(" "))
            # Check if line is a list item
            match = re.match(r"\s*[-*]\s*(.+)", line)

            if not match:
                continue

            content = match.group(1).strip()
            level = indent // 2

            stack = stack[:level]  # rewind stack to current level
            link_match = re.match(r"\[([^\]]+)\]\(([^)]+)\)", content)

            if link_match:
                title, url = link_match.groups()
                stack.append(title)
                entries.append((tuple(stack), url))
                stack.pop()  # remove the leaf so siblings at same level work
            else:
                # section header without link
                stack.append(content)

    return entries


def process_wiki_content(
    filename: str, folder_base: str, index_dict: Dict[str, str]
) -> None:
    """
    Process a wiki file and write converted output in GH Pages format.

    Args:
        filename: The name of the wiki file to process (an .md file).
        folder_base: The folder name under developer-documentation where the file will be written.
        index_dict: A mapping of page names to their new GH Pages location.
    """

    with open(filename, "r", encoding="utf-8") as f:
        content = f.read()

    # Replace GitHub Wiki links with GitHub Pages links
    # Wiki links point to other wiki pages but we need to point to the GitHub Pages site
    content = re.sub(wiki_link_base, github_pages_base, content)

    # Update links based on index dictionary containing page names and the new page location
    for page_name, new_page_location in index_dict.items():
        content = re.sub(f"/{page_name}", f"/{new_page_location}", content)

    # Convert [[Link Name]] to [Link Name](Link-Name.html)
    content = re.sub(
        r"\[ \[([^\|\]]+)\]\]",
        lambda m: f"[{m.group(1)}]({m.group(1).replace(' ', '-')}.html)",
        content,
    )
    # Convert [[Link Text|Page Name]] to [Link Text](Page-Name.html)
    content = re.sub(
        r"\[\[([^\|\]]+)\|([^\|\]]+)\]\]",
        lambda m: f"[{m.group(1)}]({m.group(2).replace(' ', '-')}.html)",
        content,
    )
    # Ensure code block fences have blank lines before and after
    content = re.sub(
        r"([^\n])(```.*?```)([^\n])",
        r"\1\n\2\n\3",
        content,
        flags=re.DOTALL,
    )
    content = re.sub(r"([^\n])(```)", r"\1\n\2", content)
    content = re.sub(r"(```)([^\n])", r"\1\n\2", content)

    # Remove checbokex in lists
    content = re.sub(r"- \[(?: |x|X)\]", "- ", content)

    # Fix leading spaces before list markers
    content = re.sub(r"^\s+-", "-", content, flags=re.MULTILINE)

    # Make sure a blank line exists before list items
    content = re.sub(r"([^\n])\n(?=\s*[-*+]\s)", r"\1\n\n", content)

    # The folder where the new files will be written and will be the base for the GH Pages wiki docs
    new_wiki_dir = Path("..") / GH_pages_folder_name

    # Make sure the folder structure exists
    os.makedirs(new_wiki_dir / folder_base, exist_ok=True)

    # Write to index file a list of pages incuded in the folder
    with open(
        os.path.join(new_wiki_dir / folder_base, "index.md"), "a", encoding="utf-8"
    ) as f:
        print(f"* [{clean_filename(filename)}]({filename}) \n", file=f)

    # Write converted content to new file in the developer-documentation folder to be displayed on GH Pages
    new_file_name = os.path.join(new_wiki_dir / folder_base, filename)

    with open(new_file_name, "a", encoding="utf-8") as f:
        f.write(content)


def clean_filename(filename: str) -> str | None:
    """
    Check if a markdown file exists, rename it
    to replace spaces with hyphens, and return the base name (without .md).

    Args:
        filename: The original filename to check and rename (with .md extension).
    """
    if (
        not os.path.exists(filename)
        or not filename.endswith(".md")
        or filename == "index.md"
    ):
        return None

    # Rename file to replace spaces with hyphens if necessary
    new_name = filename.replace(" ", "-")
    if filename != new_name:
        os.rename(filename, new_name)

    # Return filename base (without .md)
    return new_name.removesuffix(".md")


def generate_index_dict(sidebar_nav: list[tuple[list[str], str]]) -> dict[str, str]:
    """
    Create the index dictionary mapping filename bases
    to their final destination paths. Renames files as a side effect.

    Args:
        sidebar_nav: The parsed sidebar navigation entries as a list of tuples.
    Returns:
        index_dict: A dictionary mapping filenames to their new paths to be displayed in GH Pages.
    """
    index_dict = {}

    for entry in sidebar_nav:
        # Only process entries with group and page (len == 2)
        if len(entry[0]) != 2:
            continue

        group, _ = entry[0]
        link = entry[1]

        folder_base = group
        filename = link.split("/")[-1] + ".md"

        standard_base_name = clean_filename(filename)

        if standard_base_name:
            # Construct the final path using the standardised base name
            new_file_name = os.path.join(
                GH_pages_folder_name, folder_base, standard_base_name
            )
            index_dict[standard_base_name] = new_file_name

    return index_dict


def generate_gh_pages(
    sidebar_nav: list[tuple[list[str], str]], index_dict: dict[str, str]
) -> None:
    """
    Generate the files GH Pages markdown files using the generated index dictionary.

    Args:
        sidebar_nav: The parsed sidebar navigation entries as a list of tuples.
        index_dict: A dictionary mapping filenames to their new paths to be displayed in GH Pages.
    """
    for entry in sidebar_nav:
        if len(entry[0]) != 2:
            continue

        group, _ = entry[0]
        link = entry[1]

        folder_base = group
        filename = link.split("/")[-1] + ".md"
        standard_base_name = clean_filename(filename)

        if standard_base_name:
            current_filename = standard_base_name + ".md"
            process_wiki_content(current_filename, folder_base, index_dict)


# Main script execution
wiki_dir = os.path.join("docs", wiki_subfolder)
os.chdir(wiki_dir)
sidebar_nav = parse_markdown_nav("_Sidebar.md")

# Generate index of page names and their GH pages location
index_dict = generate_index_dict(sidebar_nav)

# Process files and generate GH pages
generate_gh_pages(sidebar_nav, index_dict)
