from pathlib import Path

def is_empty_folder(path: Path):
    return path.exists() and not any(path.iterdir())