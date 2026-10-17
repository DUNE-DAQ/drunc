from pathlib import Path


def on_nav(env, config, files):
    for file in files:
        if file.page:
            # Restore origin file name (without extension) as title.
            if "index" in str(Path(file.src_uri).stem):
                continue
            if "README" in str(Path(file.src_uri).stem):
                continue
            file.page.title = Path(file.src_uri).stem.replace("-", " ")
