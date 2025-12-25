import argparse
import os
import struct
import subprocess
import time
import yaml
import shutil
import pickle
import heapq
from collections import defaultdict
from contextlib import ExitStack
from pymongo import MongoClient
from typing import List, Dict, Set, Iterator, Tuple, Optional
from . import compression
from .tokenizer_wrapper import TokenizerClient

MAGIC_DOCS = b'DOCS'
MAGIC_DICT = b'DICT'
MAGIC_POST = b'POST'
VERSION = 3

BLOCK_SIZE = 5000

def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}

def connect_collection(cfg: dict):
    db_cfg = cfg.get("db") or {}
    conn = db_cfg.get("connection_string", "mongodb://localhost:27017/")
    db_name = db_cfg.get("database", "ir_search")
    return MongoClient(conn)[db_name][db_cfg.get("collection", "documents")]

def write_docs_index(doc_infos: List[Tuple[str, str]], out_dir: str):
    path = os.path.join(out_dir, "index.docs")
    count = len(doc_infos)
    data_buffer = bytearray()
    offsets = []
    
    for url, title in doc_infos:
        offsets.append(len(data_buffer))
        
        encoded_url = url.encode('utf-8')[:65535]
        data_buffer.extend(struct.pack('<H', len(encoded_url)))
        data_buffer.extend(encoded_url)
        
        encoded_title = title.encode('utf-8')[:65535] if title else b""
        data_buffer.extend(struct.pack('<H', len(encoded_title)))
        data_buffer.extend(encoded_title)

    with open(path, "wb") as f:
        f.write(MAGIC_DOCS)
        f.write(struct.pack('<H', VERSION))
        f.write(struct.pack('<I', count))
        base_offset = f.tell() + (count * 8)
        for off in offsets:
            f.write(struct.pack('<Q', base_offset + off))
        f.write(data_buffer)

def save_temp_block(index: Dict[str, Dict[int, List[int]]], block_id: int, temp_dir: str):
    path = os.path.join(temp_dir, f"block_{block_id}.tmp")
    sorted_terms = sorted(index.keys())
    
    with open(path, "wb") as f:
        for term in sorted_terms:
            pickle.dump((term, index[term]), f)

def block_reader(file_path: str) -> Iterator[Tuple[str, Dict[int, List[int]]]]:
    with open(file_path, "rb") as f:
        while True:
            try:
                yield pickle.load(f)
            except EOFError:
                break

def merge_blocks(temp_dir: str, num_blocks: int, out_dir: str):
    dict_path = os.path.join(out_dir, "index.dict")
    post_path = os.path.join(out_dir, "index.postings")
    
    block_files = [os.path.join(temp_dir, f"block_{i}.tmp") for i in range(num_blocks)]
    iterators = [block_reader(p) for p in block_files]
    
    merged_stream = heapq.merge(*iterators, key=lambda x: x[0])
    
    with open(dict_path, "wb") as f_dict, open(post_path, "wb") as f_post:
        f_dict.write(MAGIC_DICT)
        f_dict.write(struct.pack('<H', VERSION))
        f_dict.write(struct.pack('<I', 0))
        
        f_post.write(MAGIC_POST)
        f_post.write(struct.pack('<H', VERSION))
        
        current_term = None
        current_docs: Dict[int, List[int]] = {}
        term_count = 0
        
        for term, doc_map in merged_stream:
            if term != current_term:
                if current_term is not None:
                    write_term_entry(current_term, current_docs, f_dict, f_post)
                    term_count += 1
                
                current_term = term
                current_docs = doc_map
            else:
                current_docs.update(doc_map)
                
        if current_term is not None:
            write_term_entry(current_term, current_docs, f_dict, f_post)
            term_count += 1
            
        f_dict.seek(6)
        f_dict.write(struct.pack('<I', term_count))

def write_term_entry(term: str, doc_map: Dict[int, List[int]], f_dict, f_post):
    doc_ids = sorted(doc_map.keys())
    doc_count = len(doc_ids)
    
    offset = f_post.tell()
    
    f_post.write(compression.encode_varbyte(doc_count))
    
    doc_deltas = compression.encode_delta(doc_ids)
    
    for i, doc_id in enumerate(doc_ids):
        f_post.write(compression.encode_varbyte(doc_deltas[i]))
        
        positions = sorted(doc_map[doc_id])
        freq = len(positions)
        
        f_post.write(compression.encode_varbyte(freq))
        
        pos_deltas = compression.encode_delta(positions)
        for pos_d in pos_deltas:
            f_post.write(compression.encode_varbyte(pos_d))
            
    term_bytes = term.encode('utf-8')[:255]
    f_dict.write(struct.pack('<B', len(term_bytes)))
    f_dict.write(term_bytes)
    f_dict.write(struct.pack('<Q', offset))
    f_dict.write(struct.pack('<I', doc_count))

def build_index(config_path: str, tokenizer_path: str, out_dir: str, limit: int):
    cfg = read_yaml(config_path)
    coll = connect_collection(cfg)
    
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)
    
    temp_dir = os.path.join(out_dir, "temp")
    os.makedirs(temp_dir, exist_ok=True)

    tokenizer = TokenizerClient(tokenizer_path)

    doc_infos: List[Tuple[str, str]] = []
    local_index: Dict[str, Dict[int, List[int]]] = defaultdict(lambda: defaultdict(list))
    
    cursor = coll.find({"parsed_text": {"$exists": True}}, {"parsed_text": 1, "url": 1, "title": 1})
    if limit > 0:
        cursor = cursor.limit(limit)

    start_time = time.time()
    docs_processed = 0
    block_id = 0
    
    try:
        for doc in cursor:
            doc_id = docs_processed
            doc_infos.append((doc.get("url", ""), doc.get("title", "")))
            
            text = doc.get("parsed_text", "")
            tokens = tokenizer.tokenize(text)
            
            for pos, token in enumerate(tokens):
                local_index[token][doc_id].append(pos)
                
            docs_processed += 1
            
            if docs_processed % BLOCK_SIZE == 0:
                save_temp_block(local_index, block_id, temp_dir)
                local_index.clear()
                block_id += 1
                
        if local_index:
            save_temp_block(local_index, block_id, temp_dir)
            block_id += 1
            
    finally:
        tokenizer.close()
        
    write_docs_index(doc_infos, out_dir)
    merge_blocks(temp_dir, block_id, out_dir)
    
    shutil.rmtree(temp_dir)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--tokenizer", default="bin/tokenizer")
    parser.add_argument("--out-dir", default="index")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    
    build_index(args.config, args.tokenizer, args.out_dir, args.limit)

if __name__ == "__main__":
    main()
