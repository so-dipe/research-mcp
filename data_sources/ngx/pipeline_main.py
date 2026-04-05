from pipeline import ingest_docs, list_docs, get_ngx_institutions

if __name__ == "__main__":
    # institutions = get_ngx_institutions()
    # print(institutions)
    docs = list_docs("NGACCESS0005")
    print(docs)

    results = ingest_docs(docs)
    print(results)