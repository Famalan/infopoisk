import argparse
import os
import struct
import time
import re
from typing import List, Set, Dict, Optional, Tuple, Any
from . import compression
from .tokenizer_wrapper import TokenizerClient

MAGIC_DOCS = b'DOCS'
MAGIC_DICT = b'DICT'
MAGIC_POST = b'POST'

class CompressedIndexReader:
    def __init__(self, index_dir: str):
        self.index_dir = index_dir
        self.doc_infos: List[Dict[str, str]] = []
        self.term_dict: Dict[str, tuple] = {}
        
        self._load_docs()
        self._load_dict()
        
        self.post_file = open(os.path.join(index_dir, "index.postings"), "rb")
        
    def _load_docs(self):
        path = os.path.join(self.index_dir, "index.docs")
        with open(path, "rb") as f:
            magic = f.read(4)
            if magic != MAGIC_DOCS:
                raise ValueError("Invalid DOCS file")
            ver = struct.unpack('<H', f.read(2))[0]
            count = struct.unpack('<I', f.read(4))[0]
            
            offsets = []
            for _ in range(count):
                offsets.append(struct.unpack('<Q', f.read(8))[0])
                
            for off in offsets:
                f.seek(off)
                
                url_len = struct.unpack('<H', f.read(2))[0]
                url = f.read(url_len).decode('utf-8')
                
                title = ""
                if ver >= 3:
                    title_len = struct.unpack('<H', f.read(2))[0]
                    title = f.read(title_len).decode('utf-8')
                
                self.doc_infos.append({"url": url, "title": title})
                
    def _load_dict(self):
        path = os.path.join(self.index_dir, "index.dict")
        with open(path, "rb") as f:
            magic = f.read(4)
            if magic != MAGIC_DICT:
                raise ValueError("Invalid DICT file")
            ver = struct.unpack('<H', f.read(2))[0]
            count = struct.unpack('<I', f.read(4))[0]
            
            for _ in range(count):
                length = struct.unpack('<B', f.read(1))[0]
                term = f.read(length).decode('utf-8')
                offset = struct.unpack('<Q', f.read(8))[0]
                doc_count = struct.unpack('<I', f.read(4))[0]
                self.term_dict[term] = (offset, doc_count)

    def get_postings(self, term: str) -> Dict[int, List[int]]:
        if term not in self.term_dict:
            return {}
            
        offset, expected_doc_count = self.term_dict[term]
        self.post_file.seek(offset)
        
        buffer_size = 1024 * 1024
        buffer = self.post_file.read(buffer_size)
        
        if not buffer:
            return {}
            
        ptr = 0
        doc_count, ptr = compression.decode_varbyte_stream(buffer, ptr)
        
        result = {}
        curr_doc_id = 0
        
        for _ in range(doc_count):
            if ptr >= len(buffer) - 10:
                new_chunk = self.post_file.read(buffer_size)
                if new_chunk:
                    buffer = buffer[ptr:] + new_chunk
                    ptr = 0
            
            doc_delta, ptr = compression.decode_varbyte_stream(buffer, ptr)
            curr_doc_id += doc_delta
            
            freq, ptr = compression.decode_varbyte_stream(buffer, ptr)
            
            positions = []
            curr_pos = 0
            for _ in range(freq):
                if ptr >= len(buffer) - 5:
                     new_chunk = self.post_file.read(buffer_size)
                     if new_chunk:
                        buffer = buffer[ptr:] + new_chunk
                        ptr = 0
                
                pos_delta, ptr = compression.decode_varbyte_stream(buffer, ptr)
                curr_pos += pos_delta
                positions.append(curr_pos)
            
            result[curr_doc_id] = positions
            
        return result

    def get_doc_info(self, doc_id: int) -> Dict[str, str]:
        if 0 <= doc_id < len(self.doc_infos):
            return self.doc_infos[doc_id]
        return {"url": "", "title": ""}
    
    def get_doc_url(self, doc_id: int) -> str:
        return self.get_doc_info(doc_id)["url"]
        
    def close(self):
        self.post_file.close()

class SearchEngine:
    def __init__(self, reader: CompressedIndexReader, tokenizer: TokenizerClient):
        self.reader = reader
        self.tokenizer = tokenizer
        self.all_docs = set(range(len(reader.doc_infos)))

    def execute(self, query: str) -> Set[int]:
        tokens = self._tokenize(query)
        return self._evaluate_recursive(tokens)

    def _tokenize(self, query: str) -> List[str]:
        q = query.replace('«', '"').replace('»', '"')
        pattern = re.compile(r'"([^"]+)"|(\d+)|(&&|\|\||!|\(|\)|/)|([^\s"&|!()/]+)')
        
        tokens = []
        for match in pattern.finditer(q):
            phrase, num, op, word = match.groups()
            if phrase:
                tokens.append(f'PHRASE:{phrase}')
            elif num:
                tokens.append(f'NUM:{num}')
            elif op:
                tokens.append(op)
            elif word:
                tokens.append(word)
        return tokens

    def _evaluate_recursive(self, tokens: List[str]) -> Set[int]:
        processed_tokens = []
        i = 0
        while i < len(tokens):
            t = tokens[i]
            if t.startswith('PHRASE:') or not (t in ('&&','||','!','(',')','/') or t.startswith('NUM:')):
                if i + 2 < len(tokens) and tokens[i+1] == '/' and tokens[i+2].startswith('NUM:'):
                    dist = int(tokens[i+2].split(':')[1])
                    content = t if not t.startswith('PHRASE:') else t[7:]
                    processed_tokens.append(('PROXIMITY', content, dist))
                    i += 3
                    continue
                else:
                    if t.startswith('PHRASE:'):
                        processed_tokens.append(('PHRASE', t[7:]))
                    else:
                        processed_tokens.append(('TERM', t))
            elif t in ('&&','||','!','(',')'):
                processed_tokens.append(('OP', t))
            else:
                pass 
            i += 1

        output = []
        stack = []
        PRECEDENCE = {'(': 0, '||': 1, '&&': 2, '!': 3}
        
        final_tokens = []
        for i, token in enumerate(processed_tokens):
            final_tokens.append(token)
            if i < len(processed_tokens) - 1:
                curr_type, curr_val = token[:2]
                next_type, next_val = processed_tokens[i+1][:2]
                
                is_op1 = (curr_type == 'OP' and curr_val != ')')
                is_op2 = (next_type == 'OP' and next_val not in ('(', '!'))
                
                if not is_op1 and not is_op2:
                    final_tokens.append(('OP', '&&'))

        for token in final_tokens:
            type_, val = token[:2]
            if type_ in ('TERM', 'PHRASE', 'PROXIMITY'):
                output.append(token)
            elif type_ == 'OP':
                if val == '(':
                    stack.append(val)
                elif val == ')':
                    while stack and stack[-1] != '(':
                        output.append(('OP', stack.pop()))
                    if stack: stack.pop()
                else:
                    while stack and stack[-1] != '(' and PRECEDENCE.get(stack[-1], 0) >= PRECEDENCE.get(val, 0):
                        output.append(('OP', stack.pop()))
                    stack.append(val)
        
        while stack:
            output.append(('OP', stack.pop()))

        return self._solve_rpn(output)

    def _solve_rpn(self, rpn: List[tuple]) -> Set[int]:
        stack = []
        for token in rpn:
            type_, val = token[:2]
            
            if type_ == 'TERM':
                stems = self.tokenizer.tokenize(val)
                if stems:
                    postings = self.reader.get_postings(stems[0])
                    stack.append(set(postings.keys()))
                else:
                    stack.append(set())
                
            elif type_ == 'PHRASE':
                terms = self.tokenizer.tokenize(val)
                docs = self._sequence_search(terms, max_dist=len(terms))
                stack.append(docs)
                
            elif type_ == 'PROXIMITY':
                terms = self.tokenizer.tokenize(val)
                dist = token[2]
                docs = self._sequence_search(terms, max_dist=dist)
                stack.append(docs)
                
            elif type_ == 'OP':
                if val == '!':
                    if not stack: continue
                    a = stack.pop()
                    stack.append(self.all_docs - a)
                elif val == '&&':
                    if len(stack) < 2: continue
                    b = stack.pop()
                    a = stack.pop()
                    stack.append(a & b)
                elif val == '||':
                    if len(stack) < 2: continue
                    b = stack.pop()
                    a = stack.pop()
                    stack.append(a | b)
        
        return stack[0] if stack else set()

    def _sequence_search(self, terms: List[str], max_dist: int) -> Set[int]:
        if not terms:
            return set()
            
        term_postings = []
        for t in terms:
            p = self.reader.get_postings(t)
            if not p: return set()
            term_postings.append(p)
            
        common_docs = set(term_postings[0].keys())
        for p in term_postings[1:]:
            common_docs &= set(p.keys())
            
        result_docs = set()
        
        for doc_id in common_docs:
            positions_list = [term_postings[i][doc_id] for i in range(len(terms))]
            if self._check_sequence(positions_list, max_dist):
                result_docs.add(doc_id)
                
        return result_docs

    def _check_sequence(self, positions_list: List[List[int]], max_dist: int) -> bool:
        is_exact = (max_dist == len(positions_list))
        return self._find_path(positions_list, 0, -1, -1, max_dist, is_exact)

    def _find_path(self, positions_list, idx, prev_pos, first_pos, max_dist, is_exact) -> bool:
        if idx == len(positions_list):
            return True
            
        candidates = positions_list[idx]
        
        for pos in candidates:
            if idx == 0:
                if self._find_path(positions_list, idx+1, pos, pos, max_dist, is_exact):
                    return True
            else:
                if pos > prev_pos:
                    if is_exact:
                        if pos != prev_pos + 1:
                            continue
                    
                    if (pos - first_pos) > max_dist:
                         continue
                    
                    if self._find_path(positions_list, idx+1, pos, first_pos, max_dist, is_exact):
                        return True
                        
        return False

def main_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index-dir", default="index")
    parser.add_argument("--query")
    parser.add_argument("--input-file")
    parser.add_argument("--output-file")
    parser.add_argument("--tokenizer", default="bin/tokenizer")
    args = parser.parse_args()
    
    tokenizer = TokenizerClient(args.tokenizer)
    
    try:
        try:
            reader = CompressedIndexReader(args.index_dir)
        except Exception as e:
            print(f"Error loading index: {e}")
            return

        engine = SearchEngine(reader, tokenizer)

        if args.input_file and args.output_file:
            try:
                with open(args.input_file, 'r', encoding='utf-8') as f_in, \
                     open(args.output_file, 'w', encoding='utf-8') as f_out:
                    
                    for line in f_in:
                        q = line.strip()
                        if not q: continue
                        
                        f_out.write(f"Query: {q}\n")
                        try:
                            results = engine.execute(q)
                            sorted_results = sorted(list(results))
                            f_out.write(f"Found: {len(results)} docs\n")
                            
                            for i, doc_id in enumerate(sorted_results[:10]):
                                info = reader.get_doc_info(doc_id)
                                title = info['title'] or "No Title"
                                url = info['url']
                                f_out.write(f"{i+1}. {title} ({url})\n")
                            
                        except Exception as e:
                            f_out.write(f"Error executing query: {e}\n")
                        f_out.write("\n")
                
            except FileNotFoundError as e:
                print(f"File error: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")

        elif args.query:
            start = time.time()
            results = engine.execute(args.query)
            elapsed = time.time() - start
            print(f"Found {len(results)} docs in {elapsed:.4f}s")
            for doc_id in sorted(list(results))[:10]:
                info = reader.get_doc_info(doc_id)
                print(f" - {info['title']} ({info['url']})")
        
        else:
            while True:
                try:
                    q = input("> ")
                    if q.strip() == "exit": break
                    start = time.time()
                    results = engine.execute(q)
                    elapsed = time.time() - start
                    print(f"Found {len(results)} docs in {elapsed:.4f}s")
                    for doc_id in sorted(list(results))[:10]:
                        info = reader.get_doc_info(doc_id)
                        print(f" - {info['title']} ({info['url']})")
                except (KeyboardInterrupt, EOFError):
                    break
                except Exception as e:
                    print(f"Error: {e}")
        
        reader.close()
    finally:
        tokenizer.close()

if __name__ == "__main__":
    main_cli()
