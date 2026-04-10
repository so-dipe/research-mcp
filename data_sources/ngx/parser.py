def get_text(d: dict, k: str) -> str:
    """
    Safely extracts text from xmltodict structures.
    """
    v = d.get(k, {})

    if isinstance(v, str):
        return v
    
    if isinstance(v, dict):
        return v.get("#text", "")
    
    return ""

def fetch_docs(entries: list) -> list:
    docs = []
    for entry in entries:
        props = entry.get("content", {}).get("m:properties", {})
        url_info = props.get("d:URL", {})

        docs.append({
            "institution": props.get("d:InternationSecIN"),
            "doc_name": get_text(url_info, "d:Description"),
            "url": url_info.get("d:Url"),
            "submission_type": get_text(props, "d:Type_of_Submission"),
            "date_modified": props.get("d:Modified", {}).get("#text")
        })

    return docs