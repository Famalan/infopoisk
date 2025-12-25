import argparse
import subprocess
import os
import yaml
from pymongo import MongoClient

def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}

def connect_collection(cfg: dict):
    db_cfg = cfg.get("db") or {}
    conn = db_cfg.get("connection_string", "mongodb://localhost:27017/")
    db_name = db_cfg.get("database", "ir_search")
    return MongoClient(conn)[db_name][db_cfg.get("collection", "documents")]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--out-dir", default="index")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    
    if not os.path.exists("bin/indexer"):
        return

    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

    cfg = read_yaml(args.config)
    coll = connect_collection(cfg)
    
    cursor = coll.find({"parsed_text": {"$exists": True}}, {"parsed_text": 1, "url": 1, "title": 1})
    if args.limit > 0:
        cursor = cursor.limit(args.limit)
    
    process = subprocess.Popen(
        ["bin/indexer", args.out_dir],
        stdin=subprocess.PIPE,
        text=True,
        bufsize=1024*1024
    )
    
    count = 0
    try:
        for doc in cursor:
            url = doc.get("url", "").replace("\t", " ").replace("\n", " ")
            title = doc.get("title", "").replace("\t", " ").replace("\n", " ")
            text = doc.get("parsed_text", "").replace("\t", " ").replace("\n", " ")
            
            line = f"{url}\t{title}\t{text}\n"
            process.stdin.write(line)
            count += 1
                
    except BrokenPipeError:
        pass
    finally:
        process.stdin.close()
        process.wait()

if __name__ == "__main__":
    main()
