import argparse
import hashlib
import json
import os
import re
import time
import urllib.parse
from html import unescape
import yaml
from pymongo import MongoClient
from typing import Optional

def now_unix() -> int:
    return int(time.time())

def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}

def strip_query(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

def repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def to_abs_path(path: str, root: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.abspath(os.path.join(root, path))

def read_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

_WS_RE = re.compile(r"\s+")
_REF_RE = re.compile(r"\[\s*\d+\s*\]")
_SCRIPT_RE = re.compile(r"(?is)<script[^>]*>.*?</script>")
_STYLE_RE = re.compile(r"(?is)<style[^>]*>.*?</style>")
_NOSCRIPT_RE = re.compile(r"(?is)<noscript[^>]*>.*?</noscript>")
_COMMENT_RE = re.compile(r"(?is)<!--.*?-->")
_TAG_RE = re.compile(r"(?is)<[^>]+>")

def extract_text_from_html(html: str) -> str:
    cleaned = _SCRIPT_RE.sub(" ", html)
    cleaned = _STYLE_RE.sub(" ", cleaned)
    cleaned = _NOSCRIPT_RE.sub(" ", cleaned)
    cleaned = _COMMENT_RE.sub(" ", cleaned)
    cleaned = _TAG_RE.sub(" ", cleaned)

    text = unescape(cleaned)
    text = _REF_RE.sub("", text)
    text = _WS_RE.sub(" ", text).strip()
    return text

def raw_to_text_path(raw_path_abs: str, raw_root_abs: str, text_root_abs: str) -> str:
    rel = os.path.relpath(raw_path_abs, raw_root_abs)
    base, _ext = os.path.splitext(rel)
    return os.path.join(text_root_abs, base + ".txt")

def build_url_from_meta(source: str, meta: dict) -> Optional[str]:
    url = meta.get("url")
    if isinstance(url, str) and url.strip():
        return strip_query(url.strip())

    doc_id = meta.get("id")
    if isinstance(doc_id, str) and doc_id.strip():
        doc_id = doc_id.strip()
        if source == "pmc":
            return f"https://pmc.ncbi.nlm.nih.gov/articles/PMC{doc_id}/"
        if source == "pubmed":
            return f"https://pubmed.ncbi.nlm.nih.gov/{doc_id}/"
        return f"{source}:{doc_id}"

    return None

def connect_collection(cfg: dict):
    db_cfg = cfg.get("db") or {}
    conn = db_cfg.get("connection_string", "mongodb://localhost:27017/")
    db_name = db_cfg.get("database", "ir_search")
    coll_name = db_cfg.get("collection", "documents")

    client = MongoClient(conn)
    db = client[db_name]
    coll = db[coll_name]

    coll.create_index("source")
    coll.create_index("fetched_at")
    return coll

def run(config_path: str, lab1_output_dir: str, meta_path: Optional[str], limit: int) -> None:
    cfg = read_yaml(config_path)
    coll = connect_collection(cfg)

    root = repo_root()
    lab1_dir_abs = to_abs_path(lab1_output_dir, root)
    raw_root_abs = os.path.join(lab1_dir_abs, "raw")
    text_root_abs = os.path.join(lab1_dir_abs, "text")

    if meta_path:
        meta_abs = to_abs_path(meta_path, root)
    else:
        meta_abs = os.path.join(lab1_dir_abs, "meta.jsonl")

    if not os.path.exists(meta_abs):
        raise FileNotFoundError(f"meta.jsonl not found: {meta_abs}")

    processed = 0
    upserted = 0
    skipped = 0
    missing_files = 0

    with open(meta_abs, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if limit > 0 and processed >= limit:
                break

            line = line.strip()
            if not line:
                continue

            try:
                meta = json.loads(line)
            except json.JSONDecodeError:
                continue

            source = meta.get("source", "")
            if not isinstance(source, str) or not source:
                source = "unknown"

            raw_path = meta.get("raw_path")
            if not isinstance(raw_path, str) or not raw_path:
                skipped += 1
                continue

            url = build_url_from_meta(source, meta)
            if not url:
                skipped += 1
                continue

            raw_abs = to_abs_path(raw_path, root)
            if not os.path.exists(raw_abs):
                missing_files += 1
                continue

            raw_bytes = read_bytes(raw_abs)
            raw_sha = sha256_hex(raw_bytes)
            raw_content = raw_bytes.decode("utf-8", errors="replace")

            text_abs = raw_to_text_path(raw_abs, raw_root_abs=raw_root_abs, text_root_abs=text_root_abs)
            if os.path.exists(text_abs):
                try:
                    parsed_text = read_bytes(text_abs).decode("utf-8", errors="replace").strip()
                except OSError:
                    parsed_text = extract_text_from_html(raw_content)
            else:
                parsed_text = extract_text_from_html(raw_content)

            fetched_at = meta.get("fetched_at")
            if not isinstance(fetched_at, int):
                fetched_at = now_unix()

            doc = {
                "url": url,
                "source": source,
                "raw_content": raw_content,
                "raw_sha256": raw_sha,
                "parsed_text": parsed_text,
                "fetched_at": fetched_at,
                "checked_at": fetched_at,
                "migrated_at": now_unix(),
                "raw_path": raw_path,
                "text_path": os.path.relpath(text_abs, root) if text_abs else None,
            }

            coll.update_one({"_id": url}, {"$set": doc}, upsert=True)
            upserted += 1
            processed += 1

    print("done")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--lab1-output-dir", default="lab1/data")
    parser.add_argument("--meta-path", default="")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    run(
        config_path=args.config,
        lab1_output_dir=args.lab1_output_dir,
        meta_path=args.meta_path or None,
        limit=int(args.limit or 0),
    )

if __name__ == "__main__":
    main()
