#!/usr/bin/env python3
import sys
import os
import subprocess
import argparse
import signal

def signal_handler(sig, frame):
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def main_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index-dir", default="index")
    parser.add_argument("--query")
    parser.add_argument("--input-file")
    parser.add_argument("--output-file")
    args = parser.parse_args()

    search_bin = "bin/search"
    if not os.path.exists(search_bin):
        return

    process = subprocess.Popen(
        [search_bin, args.index_dir],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    ready_line = process.stdout.readline()
    if "Ready" not in ready_line:
        return

    is_interactive = not args.query and not args.input_file and sys.stdin.isatty()

    def run_query(query_text):
        query_text = query_text.strip()
        if not query_text: return None, []
        
        try:
            process.stdin.write(query_text + "\n")
            process.stdin.flush()
            
            results = []
            header = process.stdout.readline().strip()
            
            while True:
                line = process.stdout.readline()
                if not line or "__END_QUERY__" in line:
                    break
                results.append(line.strip())
                
            return header, results
        except BrokenPipeError:
            sys.exit(1)

    def handle_output(query, header, results, out_f=None):
        if out_f:
            out_f.write(f"Query: {query}\n")
            if header:
                out_f.write(f"{header}\n")
                for i, r in enumerate(results):
                    out_f.write(f"{i+1}. {r}\n")
            else:
                out_f.write("Error or no results.\n")
            out_f.write("\n")
        else:
            if not is_interactive:
                print(f"Query: {query}")
                print(header)
                for i, r in enumerate(results):
                    print(f"{i+1}. {r}")
                print("-" * 40)
            else:
                print(header)
                for r in results:
                    print(f" - {r}")

    try:
        if args.query:
            header, res = run_query(args.query)
            handle_output(args.query, header, res)

        elif args.input_file:
            out_f = open(args.output_file, 'w', encoding='utf-8') if args.output_file else None
            try:
                with open(args.input_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        q = line.strip()
                        if not q: continue
                        header, res = run_query(q)
                        handle_output(q, header, res, out_f)
            finally:
                if out_f: out_f.close()

        else:
            for line in sys.stdin:
                q = line.strip()
                if q == "exit": break
                if not q: continue
                
                header, res = run_query(q)
                handle_output(q, header, res)
                
                if is_interactive:
                    print("> ", end='', flush=True)

    except KeyboardInterrupt:
        pass
    finally:
        process.terminate()

if __name__ == "__main__":
    main_cli()
