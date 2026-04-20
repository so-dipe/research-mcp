import json
import hashlib
from pathlib import Path

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

def load_bundle_artifact(bundle: Path) -> dict:
    path = bundle / "artifacts.json"

    if not path.exists():
        return {
            "doc_id": bundle.name,
            "table_count": 0,
            "tables": [],
            "processed_at": ""
        }

    with open(path, "r") as f:
        return json.load(f)
    
def save_bundle_artifact(artifacts: dict, bundle: Path):
    with open(bundle / "artifacts.json", "w") as f:
        json.dump(artifacts, f, indent=4)

def hash_content(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def generate_doc_id(doc_info: dict) -> str:
    base = (
        f"{doc_info.get('url')}"
        f"_{doc_info.get('submission_type')}"
        f"_{doc_info.get('date_modified')}"
    )
    return hashlib.md5(base.encode()).hexdigest()

def normalize_submission_type(submission_type: str) -> str:
    if not submission_type:
        return "unknown"
    
    st = submission_type.strip().lower()

    mapping = {
        "corporate action": "Corporate Actions",
        "corporate actions": "Corporate Actions",
        "corprorate actions": "Corporate Actions",
        "financial statements": "Financial Statements",
        "financial statement": "Financial Statements",
        "financial statemets": "Financial Statements",
        "directorsdealings": "Directors Dealings",
        "directors dealings": "Directors Dealings",
        "board meeting (bm)": "Board Meeting",
        "board meeting": "Board Meeting",
        "completion board meeting (cbm)": "Board Meeting",
        "annual general meeting (agm)": "Annual General Meeting",
        "annual general meeting": "Annual General Meeting",
        "earningforcast": "Earnings Forecast",
        "earnings forecast": "Earnings Forecast",
        "extra-ordinary general meeting (egm)": "Extra-Ordinary General Meeting",
        "court order meeting (com)": "Court Order Meeting",
    }

    if st in mapping:
        return mapping[st]
    
    if "corporate action" in st:
        return "Corporate Actions"
    if "financial statement" in st:
        return "Financial Statements"
    if "directors dealing" in st:
        return "Directors Dealings"

    return st.title()

def get_enrichment_level(submission_type: str) -> str:
    st = normalize_submission_type(submission_type)

    if st in ["Financial Statements"]:
        return "high"
    if st in ["Corporate Actions"]:
        return "medium"
    if st in ["Director Dealings"]:
        return "low"
    return "low"
