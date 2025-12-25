#!/usr/bin/env python3

import os
import re
import time
import json
import urllib.request
import urllib.parse
from pathlib import Path

OUTPUT_DIR = Path("data/raw/wikipedia")
TEXT_OUTPUT_DIR = Path("data/text/wikipedia")
MAX_DOCS = 5000
SLEEP_BETWEEN_REQUESTS = 0.1

SEARCH_QUERIES = [
    "CRISPR",
    "Cas9",
    "gene editing",
    "prime editing",
    "base editing",
    "genome editing",
    "CRISPR-Cas9",
    "genetic engineering",
    "gene therapy",
    "DNA repair",
]

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"

def search_articles(query: str, sroffset: int = 0) -> tuple[list[dict], int]:
    params = {
        'action': 'query',
        'list': 'search',
        'srsearch': query,
        'srlimit': 500,
        'sroffset': sroffset,
        'srwhat': 'text',
        'format': 'json',
    }
    url = f"{WIKIPEDIA_API}?{urllib.parse.urlencode(params)}"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'MAI-IR-Lab/1.0 (educational project)'})
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"  Ошибка API: {e}")
        return [], -1
    results = data.get('query', {}).get('search', [])
    cont = data.get('continue', {}).get('sroffset', -1)
    return results, cont

def get_article_text(title: str) -> tuple[str, str]:
    params = {
        'action': 'query',
        'titles': title,
        'prop': 'extracts|info',
        'explaintext': 1,
        'exsectionformat': 'plain',
        'inprop': 'url',
        'format': 'json',
    }
    url = f"{WIKIPEDIA_API}?{urllib.parse.urlencode(params)}"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'MAI-IR-Lab/1.0 (educational project)'})
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"    Ошибка загрузки {title}: {e}")
        return "", ""
    pages = data.get('query', {}).get('pages', {})
    for page_id, page in pages.items():
        if page_id == '-1':
            return "", ""
        text = page.get('extract', '')
        page_url = page.get('fullurl', f"https://en.wikipedia.org/wiki/{urllib.parse.quote(title)}")
        return text, page_url
    return "", ""

def save_article(title: str, text: str, url: str, category: str) -> bool:
    if not text or len(text.split()) < 500:
        return False
    safe_title = re.sub(r'[^\w\s-]', '', title).strip().replace(' ', '_')[:100]
    text_filepath = TEXT_OUTPUT_DIR / f"{safe_title}.txt"
    raw_filepath = OUTPUT_DIR / f"{safe_title}.json"
    if text_filepath.exists():
        return False
    metadata = {
        'title': title,
        'url': url,
        'category': category,
        'word_count': len(text.split()),
    }
    with open(raw_filepath, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    content = []
    content.append(f"Title: {title}")
    content.append(f"URL: {url}")
    content.append(f"Source: Wikipedia")
    content.append(f"Category: {category}")
    content.append("")
    content.append(text)
    with open(text_filepath, 'w', encoding='utf-8') as f:
        f.write('\n'.join(content))
    return True

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEXT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    total_saved = 0
    seen_titles = set()
    for f in TEXT_OUTPUT_DIR.glob("*.txt"):
        seen_titles.add(f.stem.lower())
    print(f"Уже скачано: {len(seen_titles)} статей")
    for query in SEARCH_QUERIES:
        if total_saved >= MAX_DOCS:
            break
        print(f"\n=== Поиск: {query} ===")
        sroffset = 0
        query_saved = 0
        while total_saved < MAX_DOCS:
            results, next_offset = search_articles(query, sroffset)
            if not results:
                print("  Больше статей нет")
                break
            print(f"  Получено {len(results)} результатов поиска")
            for result in results:
                if total_saved >= MAX_DOCS:
                    break
                title = result.get('title', '')
                if not title or ':' in title:
                    continue
                safe_title = re.sub(r'[^\w\s-]', '', title).strip().replace(' ', '_')[:100]
                if safe_title.lower() in seen_titles:
                    continue
                print(f"    Скачивание: {title[:50]}...")
                text, url = get_article_text(title)
                if save_article(title, text, url, query):
                    seen_titles.add(safe_title.lower())
                    query_saved += 1
                    total_saved += 1
                    word_count = len(text.split())
                    print(f"      Сохранено ({word_count} слов). Всего: {total_saved}")
                else:
                    print(f"      Пропуск (слишком короткая)")
                time.sleep(SLEEP_BETWEEN_REQUESTS)
            if next_offset < 0:
                break
            sroffset = next_offset
        print(f"  Сохранено по запросу: {query_saved}")
    print(f"\n=== Итого сохранено: {total_saved} статей ===")
    print(f"Всего в папке: {len(list(TEXT_OUTPUT_DIR.glob('*.txt')))} статей")

if __name__ == "__main__":
    main()

