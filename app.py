import os
import sys
import subprocess
import time
from flask import Flask, render_template, request

app = Flask(__name__)

class SearchEngine:
    def __init__(self):
        self.process = None
        self.start()
        
    def start(self):
        if not os.path.exists("bin/search"):
            return
            
        self.process = subprocess.Popen(
            ["bin/search", "index"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        line = self.process.stdout.readline()
        
    def search(self, query):
        if not self.process or self.process.poll() is not None:
            self.start()
            
        try:
            self.process.stdin.write(query + "\n")
            self.process.stdin.flush()
            
            results = []
            header = self.process.stdout.readline()
            if not header or not header.startswith("Found"): 
                return [], 0
            
            try:
                count_str = header.split()[1]
                total = int(count_str) if count_str.isdigit() else 0
            except (IndexError, ValueError):
                total = 0
            
            while True:
                line = self.process.stdout.readline()
                if not line or "__END_QUERY__" in line:
                    break
                
                parts = line.strip().rsplit(' (', 1)
                if len(parts) == 2:
                    title = parts[0]
                    url = parts[1][:-1]
                    if not title: title = url
                    results.append({'title': title, 'url': url})
                else:
                    results.append({'title': line.strip(), 'url': '#'})
                    
            return results, total
        except Exception as e:
            return [], 0

engine = SearchEngine()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search')
def search():
    query = request.args.get('q', '')
    
    if not query:
        return render_template('index.html')

    start_time = time.time()
    results, total = engine.search(query)
    elapsed = round(time.time() - start_time, 4)
    
    return render_template('results.html', 
                           query=query, 
                           results=results, 
                           total=total, 
                           elapsed=elapsed,
                           next_page=None)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
