import json
import hashlib

from .constants import MANIFESTS_DIR

MANIFEST_PATH = MANIFESTS_DIR / "ngx_manifest.json"

def load_manifest() -> dict[str, dict]:
    MANIFESTS_DIR.mkdir(parents=True, exist_ok=True)

    if not MANIFEST_PATH.exists():
        return {"documents": {}}
    
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

