"""
Script to covert wiki links to be used in Github Pages Docs. GitHub Wiki uses a Wiki-style link while GitHub Pages uses standard Markdown links.
"""

import os
import re
import sys

base_url = sys.argv[1].rstrip("/")
wiki_subfolder = sys.argv[2].strip("/")

github_pages_base = base_url + "/" + wiki_subfolder + "/"
wiki_link_base = r"https://github.com/DUNE-DAQ/drunc/wiki"

def parse_markdown_nav(file_path):
    stack = []
    entries = []
    with open(file_path) as f:
        for line in f:
            indent = len(line) - len(line.lstrip(" "))
            match = re.match(r"\s*[-*]\s*(.+)", line)
            if not match:
                continue
            content = match.group(1).strip()
            link_match = re.match(r"\[([^\]]+)\]\(([^)]+)\)", content)
            if link_match:
                title, url = link_match.groups()
                level = indent // 4  
                stack = stack[:level]
                stack.append(title)
                entries.append((tuple(stack), url))
    return entries

def process_files(filename, folder_base):
    if os.path.exists(filename):
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
        # Ensure code block fences have blank lines before and after
        content = re.sub(
            r"([^\n])(```.*?```)([^\n])",
            r"\1\n\2\n\3",
            content,
            flags=re.DOTALL,
        )
        content = re.sub(r"([^\n])(```)", r"\1\n\2", content)
        content = re.sub(r"(```)([^\n])", r"\1\n\2", content)

        if "_Sidebar" in filename:
            filename = filename.replace("_Sidebar", "index")

        if filename == "index.md":
            print("index", content)    

        
        with open(f"{folder_base}/{filename}", "w", encoding="utf-8") as f:
            f.write(content)

# Change working directory to wiki subfolder
wiki_dir = os.path.join("docs", wiki_subfolder)
os.chdir(wiki_dir)

sidebar_nav = parse_markdown_nav("_Sidebar.md")
print(sidebar_nav)

for entry in sidebar_nav:

    if len(entry[0]) == 2:
        group, page = entry[0]
        link = entry[1]
        print(f"Group: {group}, Page: {page}, Link: {link}")

        folder_base =f"{group}/{page}" 
        filename = link.split("/")[-1] + ".md"

        if filename.endswith(".md"):
            if filename != "index.md":
                new_name = filename.replace(" ", "-")
                if filename != new_name:
                    os.rename(filename, new_name)
                filename = new_name
            process_files(filename, folder_base) 
