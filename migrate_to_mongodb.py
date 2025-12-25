#!/usr/bin/env python3
"""Миграция скачанных файлов в MongoDB."""

import os
import json
import hashlib
import time
from pathlib import Path
from pymongo import MongoClient

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def now_unix() -> int:
    return int(time.time())

def migrate_wikipedia(coll, data_dir: Path):
    """Загрузка Wikipedia статей в MongoDB."""
    text_dir = data_dir / "text" / "wikipedia"
    raw_dir = data_dir / "raw" / "wikipedia"
    
    if not text_dir.exists():
        print("Wikipedia text directory not found")
        return 0
    
    count = 0
    existing = 0
    
    for txt_file in text_dir.glob("*.txt"):
        title = txt_file.stem.replace("_", " ")
        url = f"https://en.wikipedia.org/wiki/{txt_file.stem}"
        
        # Проверяем, есть ли уже в базе
        if coll.find_one({"_id": url}):
            existing += 1
            continue
        
        # Читаем текст
        parsed_text = txt_file.read_text(encoding="utf-8", errors="replace")
        
        # Читаем raw JSON если есть
        raw_file = raw_dir / f"{txt_file.stem}.json"
        if raw_file.exists():
            raw_content = raw_file.read_text(encoding="utf-8", errors="replace")
        else:
            raw_content = parsed_text
        
        raw_sha = sha256_hex(raw_content.encode("utf-8"))
        now = now_unix()
        
        doc = {
            "_id": url,
            "url": url,
            "source": "wikipedia",
            "raw_content": raw_content,
            "raw_sha256": raw_sha,
            "parsed_text": parsed_text,
            "fetched_at": now,
            "checked_at": now,
            "title": title,
            "method": "wikipedia_api"
        }
        
        coll.insert_one(doc)
        count += 1
        
        if count % 500 == 0:
            print(f"  Загружено: {count}")
    
    print(f"Wikipedia: загружено {count}, пропущено (уже есть) {existing}")
    return count

def migrate_biorxiv(coll, data_dir: Path):
    """Загрузка bioRxiv статей в MongoDB."""
    raw_dir = data_dir / "raw" / "biorxiv"
    
    if not raw_dir.exists():
        print("bioRxiv directory not found")
        return 0
    
    count = 0
    existing = 0
    
    for json_file in raw_dir.glob("*.json"):
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Error reading {json_file}: {e}")
            continue
        
        doi = data.get("doi", json_file.stem.replace("_", "/"))
        url = f"https://www.biorxiv.org/content/{doi}"
        
        # Проверяем, есть ли уже в базе
        if coll.find_one({"_id": url}):
            existing += 1
            continue
        
        raw_content = json.dumps(data, ensure_ascii=False)
        parsed_text = data.get("abstract", "")
        title = data.get("title", doi)
        
        raw_sha = sha256_hex(raw_content.encode("utf-8"))
        now = now_unix()
        
        doc = {
            "_id": url,
            "url": url,
            "source": "biorxiv",
            "raw_content": raw_content,
            "raw_sha256": raw_sha,
            "parsed_text": parsed_text,
            "fetched_at": now,
            "checked_at": now,
            "title": title,
            "method": "biorxiv_api",
            "doi": doi
        }
        
        coll.insert_one(doc)
        count += 1
        
        if count % 100 == 0:
            print(f"  Загружено: {count}")
    
    print(f"bioRxiv: загружено {count}, пропущено (уже есть) {existing}")
    return count

def main():
    # Подключение к MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ir_search"]
    coll = db["documents"]
    
    # Создаём индексы
    coll.create_index("source")
    coll.create_index("fetched_at")
    
    data_dir = Path(__file__).parent / "data"
    
    print("=== Миграция в MongoDB ===")
    print(f"До миграции: {coll.count_documents({})} документов")
    
    # Мигрируем Wikipedia
    print("\n--- Wikipedia ---")
    wiki_count = migrate_wikipedia(coll, data_dir)
    
    # Мигрируем bioRxiv
    print("\n--- bioRxiv ---")
    bio_count = migrate_biorxiv(coll, data_dir)
    
    print(f"\n=== Итого ===")
    print(f"После миграции: {coll.count_documents({})} документов")
    
    # Статистика по источникам
    pipeline = [{"$group": {"_id": "$source", "count": {"$sum": 1}}}]
    print("\nПо источникам:")
    for r in coll.aggregate(pipeline):
        print(f"  {r['_id']}: {r['count']}")

if __name__ == "__main__":
    main()


