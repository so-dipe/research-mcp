def get_text(d: dict, k: str) -> str:
    v = d.get(k, {})

    if isinstance(v, str):
        return v
    
    if isinstance(v, dict):
        return v.get("#text", "")