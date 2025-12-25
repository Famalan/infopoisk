#!/usr/bin/env python3

import os
import time
import subprocess
from pathlib import Path

def main():
    text_dirs = [
        Path("data/text/pmc"),
        Path("data/text/wikipedia")
    ]
    
    files = []
    total_bytes = 0
    for d in text_dirs:
        if d.exists():
            for f in d.glob("*.txt"):
                files.append(f)
                total_bytes += f.stat().st_size
    
    print(f"=== Статистика корпуса ===")
    print(f"Файлов: {len(files)}")
    print(f"Объём: {total_bytes / 1024 / 1024:.2f} MB")
    
    sample_files = files[:1000]
    sample_bytes = sum(f.stat().st_size for f in sample_files)
    
    print(f"\n=== Токенизация выборки ({len(sample_files)} файлов, {sample_bytes/1024:.1f} KB) ===")
    
    texts = []
    for f in sample_files:
        texts.append(f.read_text(encoding="utf-8", errors="replace"))
    combined = "\n".join(texts)
    
    start = time.time()
    result = subprocess.run(
        ["./tokenizer"],
        input=combined,
        capture_output=True,
        text=True
    )
    elapsed = time.time() - start
    
    tokens = [t for t in result.stdout.strip().split("\n") if t and t != "__END_DOC__"]
    
    print(f"\n=== Результаты ===")
    print(f"Количество токенов: {len(tokens):,}")
    if tokens:
        avg_len = sum(len(t) for t in tokens) / len(tokens)
        print(f"Средняя длина токена: {avg_len:.2f} символов")
    print(f"Время токенизации: {elapsed:.3f} сек")
    print(f"Скорость: {sample_bytes / 1024 / elapsed:.2f} KB/сек")
    
    unique = set(tokens)
    print(f"Уникальных токенов: {len(unique):,}")
    
    print(f"\n=== Примеры токенов (первые 20) ===")
    for t in tokens[:20]:
        print(f"  {t}")

if __name__ == "__main__":
    main()

