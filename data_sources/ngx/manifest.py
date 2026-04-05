import json
import hashlib
from pathlib import Path

MANIFEST_PATH = Path(__file__).resolve().parent / "data/manifest.json"

def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        return {"documents": {}}
        # raise FileNotFoundError(f"Manifest file not found at {MANIFEST_PATH}")
    
    with open(MANIFEST_PATH, "r") as f:
        return json.load(f)

def save_manifest(manifest: dict):
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)

def hash_content(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def generate_doc_id(doc_info: dict) -> str:
    base = (
        f"{doc_info.get('url')}"
        f"_{doc_info.get('submission_type')}"
        f"_{doc_info.get('date_modified')}"
    )
    return hashlib.md5(base.encode()).hexdigest()

