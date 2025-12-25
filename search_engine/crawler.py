import argparse
import gzip
import hashlib
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from html import unescape
from typing import Optional
import yaml
from pymongo import MongoClient

def now_unix() -> int:
    return int(time.time())

def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def strip_query(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

def normalize_url(url: str) -> Optional[str]:
    if not isinstance(url, str):
        return None
    url = url.strip()
    if not url:
        return None

    parts = urllib.parse.urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        return None

    netloc = parts.netloc.lower()
    path = parts.path or "/"
    query = parts.query
    fragment = ""
    return urllib.parse.urlunsplit((scheme, netloc, path, query, fragment))

def is_allowed_domain(url: str, allowed_domains: list[str]) -> bool:
    try:
        netloc = urllib.parse.urlsplit(url).netloc.lower()
    except Exception:
        return False

    host = netloc.split("@")[-1].split(":")[0]
    if not host:
        return False

    for d in allowed_domains:
        d = str(d).lower().strip()
        if not d:
            continue
        if host == d:
            return True
        if host.endswith("." + d):
            return True
    return False

_SKIP_EXT = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".pdf", ".zip", ".rar", 
    ".7z", ".tar", ".gz", ".mp3", ".mp4", ".avi", ".mov", ".css", ".js", ".ico"
)

def looks_like_html_url(url: str) -> bool:
    try:
        path = urllib.parse.urlsplit(url).path.lower()
    except Exception:
        return False
    for ext in _SKIP_EXT:
        if path.endswith(ext):
            return False
    return True

_HREF_RE = re.compile(r"""(?is)href\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s"'<>]+))""")
_META_CHARSET_RE = re.compile(r"""(?is)charset\s*=\s*["']?\s*([a-zA-Z0-9_\-]+)\s*""")
_TITLE_RE = re.compile(r"""(?is)<title[^>]*>(.*?)</title>""")

def extract_links(html: str, base_url: str) -> list[str]:
    out: list[str] = []
    for m in _HREF_RE.finditer(html):
        href = m.group(1) or m.group(2) or m.group(3) or ""
        href = href.strip()
        if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
            continue

        abs_url = urllib.parse.urljoin(base_url, href)
        abs_url = normalize_url(abs_url)
        if abs_url:
            out.append(abs_url)
    return out

def guess_charset_from_headers(resp) -> Optional[str]:
    try:
        return resp.headers.get_content_charset()
    except Exception:
        return None

def guess_charset_from_html_prefix(body_bytes: bytes) -> Optional[str]:
    prefix = body_bytes[:4096].decode("ascii", errors="ignore")
    m = _META_CHARSET_RE.search(prefix)
    if not m:
        return None
    return (m.group(1) or "").strip() or None

_DEFAULT_WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

def fetch_text(url: str, timeout_s: int, retries: int, sleep_before_retry_s: float) -> tuple[int, dict, str]:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=_DEFAULT_WEB_HEADERS, method="GET")
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                body = resp.read()
                enc = resp.headers.get("Content-Encoding", "")
                if isinstance(enc, str) and "gzip" in enc.lower():
                    try:
                        body = gzip.decompress(body)
                    except Exception:
                        pass

                charset = guess_charset_from_headers(resp) or guess_charset_from_html_prefix(body) or "utf-8"
                try:
                    text = body.decode(charset, errors="replace")
                except LookupError:
                    text = body.decode("utf-8", errors="replace")
                return 200, dict(resp.headers.items()), text
        except urllib.error.HTTPError as e:
            if e.code == 304:
                return 304, dict(getattr(e, "headers", {}).items()), ""
            last_err = e
            time.sleep(sleep_before_retry_s * attempt)
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            time.sleep(sleep_before_retry_s * attempt)

    if last_err:
        raise last_err
    raise RuntimeError("unknown error")

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

def extract_title(html: str) -> Optional[str]:
    m = _TITLE_RE.search(html)
    if m:
        title = unescape(m.group(1))
        return _WS_RE.sub(" ", title).strip()
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

def is_fresh(doc: Optional[dict], fresh_seconds: int, now: int) -> bool:
    if not doc or fresh_seconds <= 0:
        return False
    fetched_at = doc.get("fetched_at")
    if not isinstance(fetched_at, int) or fetched_at <= 0:
        return False
    return (now - fetched_at) < fresh_seconds

def upsert_doc(
    coll,
    url: str,
    source: str,
    raw_content: str,
    raw_sha256: str,
    parsed_text: str,
    fetched_at: int,
    title: Optional[str] = None,
    extra: Optional[dict] = None,
) -> None:
    doc = {
        "url": url,
        "source": source,
        "raw_content": raw_content,
        "raw_sha256": raw_sha256,
        "parsed_text": parsed_text,
        "fetched_at": fetched_at,
        "checked_at": fetched_at,
    }
    if title:
        doc["title"] = title
        
    if isinstance(extra, dict) and extra:
        doc.update(extra)
    coll.update_one({"_id": url}, {"$set": doc}, upsert=True)

def only_touch_checked_at(coll, url: str, ts: int) -> None:
    coll.update_one({"_id": url}, {"$set": {"checked_at": ts}})

def crawl_web_source(cfg: dict, src: dict, coll) -> None:
    logic = cfg.get("logic") or {}

    name = str(src.get("name", "web")).strip()
    seeds = src.get("seeds") or []
    allowed_domains = src.get("allowed_domains") or []
    doc_url_regex = src.get("doc_url_regex") or ""
    follow_url_regex = src.get("follow_url_regex") or ""

    max_docs = int(src.get("max_docs", 0) or 0)
    max_pages_total = int(src.get("max_pages_total", logic.get("max_pages_total", 0) or 0) or 0)

    sleep_ms = int(src.get("sleep_ms", logic.get("sleep_ms", 350) or 350) or 0)
    timeout_s = int(src.get("timeout_s", logic.get("timeout_s", 30) or 30) or 30)
    retries = int(src.get("retries", logic.get("retries", 3) or 3) or 3)
    fresh_seconds = int(src.get("fresh_seconds", logic.get("fresh_seconds", 0) or 0) or 0)

    doc_re = re.compile(doc_url_regex)
    follow_re = re.compile(follow_url_regex) if isinstance(follow_url_regex, str) and follow_url_regex else None

    existing_count = coll.count_documents({"source": name})
    if max_docs > 0 and existing_count >= max_docs:
        return

    queue = deque()
    visited: set[str] = set()
    for s in seeds:
        u = normalize_url(str(s))
        if u:
            queue.append(u)

    docs_saved = 0
    pages_visited = 0

    while queue:
        if max_pages_total > 0 and pages_visited >= max_pages_total:
            break
        if max_docs > 0 and (existing_count + docs_saved) >= max_docs:
            break

        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        if not is_allowed_domain(url, allowed_domains=allowed_domains) or not looks_like_html_url(url):
            continue

        is_doc = bool(doc_re.search(url)) or bool(doc_re.search(strip_query(url)))
        if follow_re and (not follow_re.search(url)) and (not is_doc):
            continue

        try:
            status, _headers, html = fetch_text(url, timeout_s=timeout_s, retries=retries, sleep_before_retry_s=1.0)
        except Exception:
            continue

        if status != 200:
            continue

        pages_visited += 1
        links = extract_links(html, base_url=url)

        for link in links:
            if link in visited or not is_allowed_domain(link, allowed_domains=allowed_domains) or not looks_like_html_url(link):
                continue
            is_doc_link = bool(doc_re.search(link)) or bool(doc_re.search(strip_query(link)))
            if follow_re and (not follow_re.search(link)) and (not is_doc_link):
                continue
            queue.append(link)

        if is_doc:
            doc_url = strip_query(url)
            now = now_unix()

            existing = coll.find_one({"_id": doc_url}, {"fetched_at": 1, "raw_sha256": 1})
            if is_fresh(existing, fresh_seconds=fresh_seconds, now=now):
                continue

            raw_bytes = html.encode("utf-8", errors="replace")
            raw_sha = sha256_hex(raw_bytes)
            if existing and existing.get("raw_sha256") == raw_sha:
                only_touch_checked_at(coll, url=doc_url, ts=now)
            else:
                parsed = extract_text_from_html(html)
                title = extract_title(html) or doc_url
                upsert_doc(
                    coll, url=doc_url, source=name, raw_content=html,
                    raw_sha256=raw_sha, parsed_text=parsed, fetched_at=now,
                    title=title, extra={"method": "web"},
                )
                docs_saved += 1

        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

_DEFAULT_NCBI_HEADERS = {
    "User-Agent": "mai-ir-lab2/1.0 (edu; contact: none)",
    "Accept": "*/*",
    "Connection": "close",
}

def http_get(url: str, timeout_s: int, retries: int) -> bytes:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=_DEFAULT_NCBI_HEADERS, method="GET")
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                body = resp.read()
                enc = resp.headers.get("Content-Encoding", "")
                if isinstance(enc, str) and "gzip" in enc.lower():
                    try:
                        body = gzip.decompress(body)
                    except Exception:
                        pass
                return body
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            time.sleep(1.0 * attempt)
    if last_err:
        raise last_err
    raise RuntimeError("unknown error")

def build_url(base: str, params: dict) -> str:
    query = urllib.parse.urlencode(params)
    return base + ("&" if "?" in base else "?") + query

def esearch_ids(base_url: str, db: str, term: str, retstart: int, retmax: int, 
                timeout_s: int, retries: int, tool: str, email: str, api_key: str) -> tuple[list[str], int]:
    params = {"db": db, "term": term, "retstart": str(retstart), "retmax": str(retmax), 
              "retmode": "json", "tool": tool}
    if email: params["email"] = email
    if api_key: params["api_key"] = api_key

    url = build_url(base_url + "/esearch.fcgi", params)
    import json
    body = http_get(url, timeout_s=timeout_s, retries=retries).decode("utf-8", errors="replace")
    data = json.loads(body)
    es = data.get("esearchresult", {})
    return [x for x in es.get("idlist", []) if x], int(es.get("count", "0"))

def efetch_xml(base_url: str, db: str, doc_id: str, timeout_s: int, retries: int, 
               tool: str, email: str, api_key: str) -> str:
    params = {"db": db, "id": doc_id, "retmode": "xml", "tool": tool}
    if email: params["email"] = email
    if api_key: params["api_key"] = api_key
    url = build_url(base_url + "/efetch.fcgi", params)
    return http_get(url, timeout_s=timeout_s, retries=retries).decode("utf-8", errors="replace")

def build_ncbi_article_url(source_name: str, doc_id: str) -> str:
    if source_name == "pmc": return f"https://pmc.ncbi.nlm.nih.gov/articles/PMC{doc_id}/"
    if source_name == "pubmed": return f"https://pubmed.ncbi.nlm.nih.gov/{doc_id}/"
    return f"{source_name}:{doc_id}"

def load_existing_ncbi_ids(coll, source_name: str) -> set[str]:
    cursor = coll.find({"source": source_name, "ncbi_id": {"$exists": True}}, {"ncbi_id": 1})
    return {doc.get("ncbi_id") for doc in cursor if doc.get("ncbi_id")}

def crawl_ncbi_eutils_source(cfg: dict, src: dict, coll) -> None:
    logic = cfg.get("logic") or {}
    ncbi = cfg.get("ncbi") or {}
    name = str(src.get("name", "ncbi")).strip()
    db = str(src.get("db", "")).strip()
    if not db: return
    terms = src.get("terms") or src.get("queries") or src.get("term")
    if isinstance(terms, str): terms = [terms]
    if not terms: return

    max_docs = int(src.get("max_docs", 0) or 0)
    page_size = int(src.get("esearch_page_size", 200) or 200)
    sleep_ms = int(src.get("sleep_ms", logic.get("sleep_ms", 350) or 350) or 0)
    timeout_s = int(src.get("timeout_s", logic.get("timeout_s", 30) or 30) or 30)
    retries = int(src.get("retries", logic.get("retries", 3) or 3) or 3)
    fresh_seconds = int(src.get("fresh_seconds", logic.get("fresh_seconds", 0) or 0) or 0)
    eutils_base = str(ncbi.get("eutils_base", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"))
    tool = str(ncbi.get("tool", "mai-ir"))
    email = str(ncbi.get("email") or os.environ.get("NCBI_EMAIL", ""))
    api_key = str(ncbi.get("api_key") or os.environ.get("NCBI_API_KEY", ""))

    existing_count = coll.count_documents({"source": name})
    if max_docs > 0 and existing_count >= max_docs: return

    already_ids = load_existing_ncbi_ids(coll, source_name=name)
    need_total = max_docs if max_docs > 0 else None
    queued, seen = [], set(already_ids)

    for term in terms:
        retstart = 0
        total = None
        while True:
            if need_total is not None and (len(already_ids) + len(queued)) >= need_total: break
            ids, total_count = esearch_ids(eutils_base, db, str(term), retstart, page_size, timeout_s, retries, tool, email, api_key)
            if total is None: total = total_count
            if not ids: break
            for doc_id in ids:
                if doc_id in seen: continue
                seen.add(doc_id)
                queued.append((doc_id, str(term)))
                if need_total is not None and (len(already_ids) + len(queued)) >= need_total: break
            retstart += page_size
            if total is not None and retstart >= total: break
            if sleep_ms > 0: time.sleep(sleep_ms / 1000.0)

    for doc_id, term in queued:
        url = build_ncbi_article_url(name, doc_id)
        now = now_unix()
        existing = coll.find_one({"_id": url}, {"fetched_at": 1, "raw_sha256": 1})
        if is_fresh(existing, fresh_seconds=fresh_seconds, now=now): continue
        try:
            xml = efetch_xml(eutils_base, db, doc_id, timeout_s, retries, tool, email, api_key)
        except Exception: continue
        raw_bytes = xml.encode("utf-8", errors="replace")
        raw_sha = sha256_hex(raw_bytes)
        if existing and existing.get("raw_sha256") == raw_sha:
            only_touch_checked_at(coll, url=url, ts=now)
        else:
            parsed = extract_text_from_html(xml)
            title = extract_title(xml) or url
            upsert_doc(coll, url=url, source=name, raw_content=xml, raw_sha256=raw_sha, parsed_text=parsed, fetched_at=now,
                       title=title, extra={"method": "ncbi_eutils", "ncbi_db": db, "ncbi_id": doc_id, "term": term})
        if sleep_ms > 0: time.sleep(sleep_ms / 1000.0)

def crawl_plos_source(cfg: dict, src: dict, coll) -> None:
    name = str(src.get("name", "plos")).strip()
    query = str(src.get("query", "CRISPR")).strip()
    max_docs = int(src.get("max_docs", 1000))
    start, docs_saved = 0, 0
    
    while docs_saved < max_docs:
        search_url = f"https://api.plos.org/search?q=everything:{urllib.parse.quote(query)}&rows=100&start={start}&wt=json"
        try:
            resp_bytes = http_get(search_url, timeout_s=30, retries=3)
            import json
            data = json.loads(resp_bytes.decode("utf-8"))
            docs = data.get("response", {}).get("docs", [])
            if not docs: break
            for doc_meta in docs:
                if docs_saved >= max_docs: break
                doi = doc_meta.get("id")
                if not doi: continue
                doc_url = f"https://journals.plos.org/plosone/article?id={doi}"
                if coll.find_one({"_id": doc_url}): continue
                try:
                    status, headers, html = fetch_text(doc_url, timeout_s=30, retries=2, sleep_before_retry_s=1.0)
                    if status == 200:
                        raw_sha = sha256_hex(html.encode("utf-8", errors="replace"))
                        parsed = extract_text_from_html(html)
                        title = doc_meta.get("title_display", extract_title(html) or doi)
                        upsert_doc(coll, url=doc_url, source=name, raw_content=html, raw_sha256=raw_sha, parsed_text=parsed, 
                                   fetched_at=now_unix(), title=title, extra={"method": "plos_api", "doi": doi})
                        docs_saved += 1
                except Exception: continue
            start += 100
            time.sleep(1)
        except Exception: break

def run(config_path: str, only_source: Optional[str]) -> None:
    cfg = read_yaml(config_path)
    coll = connect_collection(cfg)
    sources = cfg.get("sources") or []
    for src in sources:
        if not isinstance(src, dict): continue
        name = str(src.get("name", "")).strip()
        if only_source and name != only_source: continue
        method = str(src.get("method", "web")).strip()
        if method == "ncbi_eutils": crawl_ncbi_eutils_source(cfg, src, coll)
        elif method == "plos": crawl_plos_source(cfg, src, coll)
        else: crawl_web_source(cfg, src, coll)

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Поисковый робот для обкачки документов"
    )
    parser.add_argument(
        "config",
        help="Путь до yaml-конфига"
    )
    parser.add_argument(
        "--source",
        default="",
        help="Имя источника для обкачки (опционально)"
    )
    args = parser.parse_args()
    run(args.config, only_source=args.source.strip() or None)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
