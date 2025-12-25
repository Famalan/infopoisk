import argparse
import collections
import time
import os
import yaml
import struct
import matplotlib.pyplot as plt
from pymongo import MongoClient
from typing import Counter
from .tokenizer_wrapper import TokenizerClient

plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'Verdana', 'DejaVu Sans']
plt.rcParams['font.style'] = 'normal'

MAGIC_DICT = b'DICT'

def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}

def connect_collection(cfg: dict):
    db_cfg = cfg.get("db") or {}
    conn = db_cfg.get("connection_string", "mongodb://localhost:27017/")
    db_name = db_cfg.get("database", "ir_search")
    return MongoClient(conn)[db_name][db_cfg.get("collection", "documents")]

def process_batch(tokenizer: TokenizerClient, texts: list[str]) -> Counter:
    if not texts:
        return collections.Counter()
    
    full_text = " ".join(texts)
    tokens = tokenizer.tokenize(full_text)
    return collections.Counter(tokens)

def get_term_stats(index_dir: str):
    dict_path = os.path.join(index_dir, "index.dict")
    if not os.path.exists(dict_path):
        return 0, 0
        
    total_terms = 0
    total_term_length = 0
    
    try:
        with open(dict_path, "rb") as f:
            magic = f.read(4)
            if magic != MAGIC_DICT:
                return 0, 0
            ver = struct.unpack('<H', f.read(2))[0]
            count = struct.unpack('<I', f.read(4))[0]
            
            total_terms = count
            for _ in range(count):
                length = struct.unpack('<B', f.read(1))[0]
                f.seek(length, 1) 
                total_term_length += length
                f.seek(12, 1)
    except Exception as e:
        return 0, 0
        
    return total_terms, total_term_length

def run(config_path: str, tokenizer_path: str, limit: int, output_image: str) -> None:
    if not os.path.exists(tokenizer_path):
        raise FileNotFoundError(f"Tokenizer not found: {tokenizer_path}")

    cfg = read_yaml(config_path)
    coll = connect_collection(cfg)
    
    total_raw_size = 0
    total_text_size = 0
    doc_count = 0
    total_tokens = 0
    total_token_length = 0
    
    cursor = coll.find({"parsed_text": {"$exists": True}})
    if limit > 0:
        cursor = cursor.limit(limit)

    total_counts = collections.Counter()
    batch_size = 500
    batch_texts = []
    
    start_time = time.time()
    tokenizer = TokenizerClient(tokenizer_path)
    
    try:
        for doc in cursor:
            doc_count += 1
            raw_content = doc.get("raw_content", "")
            parsed_text = doc.get("parsed_text", "")
            
            total_raw_size += len(raw_content)
            total_text_size += len(parsed_text)
            
            if parsed_text:
                batch_texts.append(parsed_text)
            
            if len(batch_texts) >= batch_size:
                c = process_batch(tokenizer, batch_texts)
                total_counts.update(c)
                
                for token, count in c.items():
                    total_token_length += len(token) * count
                    total_tokens += count
                
                batch_texts = []

        if batch_texts:
            c = process_batch(tokenizer, batch_texts)
            total_counts.update(c)
            for token, count in c.items():
                total_token_length += len(token) * count
                total_tokens += count
                
    finally:
        tokenizer.close()

    elapsed = time.time() - start_time
    
    index_dir = "index"
    total_terms, total_term_length = get_term_stats(index_dir)
    
    print("\n" + "="*30)
    print("=== Lab 1 Stats ===")
    print(f"Total docs: {doc_count}")
    print(f"Total raw size: {total_raw_size} bytes ({total_raw_size/1024/1024:.2f} MB)")
    print(f"Total text size: {total_text_size} bytes ({total_text_size/1024/1024:.2f} MB)")
    if doc_count > 0:
        print(f"Avg doc raw size: {total_raw_size / doc_count:.2f} bytes")
        print(f"Avg doc text size: {total_text_size / doc_count:.2f} bytes")
    
    print("\n=== Lab 3 & 6 Stats ===")
    print(f"Total tokens: {total_tokens}")
    if total_tokens > 0:
        print(f"Avg token length: {total_token_length / total_tokens:.2f}")
    
    print(f"Total terms (unique): {total_terms}")
    if total_terms > 0:
        print(f"Avg term length: {total_term_length / total_terms:.2f}")
        
    if total_tokens > 0 and total_terms > 0:
        diff = (total_token_length / total_tokens) - (total_term_length / total_terms)
        print(f"Difference (Token len - Term len): {diff:.2f}")
        
    print(f"Tokenization speed: {total_text_size / 1024 / elapsed:.2f} KB/s")
    print("="*30 + "\n")

    if not total_counts:
        return

    sorted_counts = total_counts.most_common()
    freqs = [count for term, count in sorted_counts]
    ranks = range(1, len(freqs) + 1)
    
    plt.figure(figsize=(10, 6))
    plt.loglog(ranks, freqs, marker=".", linestyle="none", label="Corpus Data")
    
    C = freqs[0]
    zipf_line = [C / r for r in ranks]
    plt.loglog(ranks, zipf_line, linestyle="--", color="red", label="Zipf's Law (C/rank)")
    
    plt.title("Zipf's Law Analysis")
    plt.xlabel("Rank (log)")
    plt.ylabel("Frequency (log)")
    plt.legend()
    plt.grid(True, which="both", ls="-", alpha=0.5)
    
    plt.savefig(output_image)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--tokenizer", default="bin/tokenizer")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output", default="zipf_plot.png")
    args = parser.parse_args()
    
    run(args.config, args.tokenizer, args.limit, args.output)

if __name__ == "__main__":
    main()
