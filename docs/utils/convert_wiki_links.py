"""
Script to covert wiki links to be used in Github Pages Docs. GitHub Wiki uses a Wiki-style link while GitHub Pages uses standard Markdown links.
"""

import os
import re
import sys

base_url = sys.argv[1].rstrip("/")
wiki_subfolder = sys.argv[2].strip("/")

github_pages_base = base_url + "/" + wiki_subfolder + "/"
wiki_link_base = r"https://github.com/DUNE-DAQ/drunc/wiki/"

# Change working directory to wiki subfolder
wiki_dir = os.path.join("docs", wiki_subfolder)
os.chdir(wiki_dir)

# Rename Home.md to index.md if it exists
if os.path.isfile("Home.md"):
    os.rename("Home.md", "index.md")

# Process all .md files
for filename in os.listdir():
    if filename.endswith(".md"):
        if filename != "index.md":
            new_name = filename.replace(" ", "-")
            if filename != new_name:
                os.rename(filename, new_name)
            filename = new_name

        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()

        # Replace GitHub wiki links with GitHub Pages links
        # WIki links point to other wiki pages but we need to point to the GitHub Pages site
        content = re.sub(wiki_link_base, github_pages_base, content)

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

        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
