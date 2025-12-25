#!/usr/bin/env python3
"""Построение графика закона Ципфа из MongoDB (потоковая обработка)."""

import collections
import subprocess
from pymongo import MongoClient
import matplotlib.pyplot as plt

BATCH_SIZE = 500

def main():
    client = MongoClient('mongodb://localhost:27017/')
    coll = client['ir_search']['documents']
    
    total_docs = coll.count_documents({})
    print(f'Всего документов в MongoDB: {total_docs}')
    
    global_counts = collections.Counter()
    total_tokens = 0
    processed = 0
    
    cursor = coll.find({'parsed_text': {'$exists': True}}, {'parsed_text': 1})
    batch = []
    
    for doc in cursor:
        text = doc.get('parsed_text', '')
        if text:
            batch.append(text)
        
        if len(batch) >= BATCH_SIZE:
            combined = '\n'.join(batch)
            result = subprocess.run(['./tokenizer'], input=combined, capture_output=True, text=True)
            tokens = [t for t in result.stdout.strip().split('\n') if t and t != '__END_DOC__']
            global_counts.update(tokens)
            total_tokens += len(tokens)
            processed += len(batch)
            print(f'  Обработано: {processed}, токенов: {total_tokens:,}')
            batch = []
    
    if batch:
        combined = '\n'.join(batch)
        result = subprocess.run(['./tokenizer'], input=combined, capture_output=True, text=True)
        tokens = [t for t in result.stdout.strip().split('\n') if t and t != '__END_DOC__']
        global_counts.update(tokens)
        total_tokens += len(tokens)
        processed += len(batch)
    
    print(f'\nИтого: {processed} документов, {total_tokens:,} токенов, {len(global_counts):,} уникальных')
    
    sorted_counts = global_counts.most_common()
    freqs = [c for _, c in sorted_counts]
    ranks = range(1, len(freqs) + 1)
    
    plt.figure(figsize=(10, 6))
    plt.loglog(ranks, freqs, marker='.', linestyle='none', markersize=2, label='Corpus Data (PMC + Wikipedia)')
    
    C = freqs[0]
    zipf_line = [C / r for r in ranks]
    plt.loglog(ranks, zipf_line, linestyle='--', color='red', linewidth=2, label="Zipf's Law (C/rank)")
    
    plt.title("Zipf's Law Analysis (32,809 documents)", fontsize=14)
    plt.xlabel('Rank (log)', fontsize=12)
    plt.ylabel('Frequency (log)', fontsize=12)
    plt.legend(fontsize=11)
    plt.grid(True, which='both', ls='-', alpha=0.3)
    plt.tight_layout()
    plt.savefig('zipf_plot.png', dpi=150)
    print('График сохранён: zipf_plot.png')
    
    print('\nТоп-10 частотных слов:')
    for word, freq in sorted_counts[:10]:
        print(f'  {word}: {freq:,}')

if __name__ == '__main__':
    main()
