#!/usr/bin/env python3
import os
import sys
import time
import subprocess

def benchmark_tokenization():
    tokenizer_path = "bin/tokenizer"
    if not os.path.exists(tokenizer_path):
        return

    sample_text = "dna crispr cas9 gene editing protein sequence mutation " * 1000
    target_size_mb = 50
    multiplier = int(target_size_mb * 1024 * 1024 / len(sample_text))
    text_chunk = sample_text * multiplier
    actual_size_mb = len(text_chunk.encode('utf-8')) / 1024 / 1024
    
    start = time.time()
    
    proc = subprocess.Popen(
        [tokenizer_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1024*1024
    )
    
    try:
        proc.stdin.write(text_chunk)
        proc.stdin.close()
        
        token_count = 0
        while True:
            line = proc.stdout.readline()
            if not line: break
            if "__END_DOC__" not in line:
                token_count += 1
        
        proc.wait()
        
    except Exception as e:
        return

    elapsed = time.time() - start
    mb_per_sec = actual_size_mb / elapsed
    
    print(f"Processed {actual_size_mb:.2f} MB in {elapsed:.4f}s")
    print(f"Speed: {mb_per_sec:.2f} MB/s")
    print(f"Tokens generated: {token_count}")

def benchmark_search_queries(index_dir):
    search_bin = "bin/search"
    if not os.path.exists(search_bin):
        return

    proc = subprocess.Popen(
        [search_bin, index_dir],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    ready = proc.stdout.readline()
    if "Ready" not in ready:
        return

    queries = [
        "protein", 
        "dna",
        "crispr",
        "\"gene editing\"",
        "protein && cell",
        "dna || rna",
        "system && !immune",
        "\"crispr cas9\" / 5"
    ]
    
    for q in queries:
        start = time.time()
        proc.stdin.write(q + "\n")
        proc.stdin.flush()
        
        header = proc.stdout.readline()
        while True:
            line = proc.stdout.readline()
            if not line or "__END_QUERY__" in line:
                break
        
        elapsed = (time.time() - start) * 1000
        print(f"{q:<30} | {elapsed:<10.2f}")
        
    proc.terminate()

def main():
    index_dir = "index"
    benchmark_tokenization()
    
    if os.path.exists(index_dir):
        benchmark_search_queries(index_dir)

if __name__ == "__main__":
    main()
