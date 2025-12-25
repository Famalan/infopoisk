#!/bin/bash
set -e

if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

source venv/bin/activate
pip install -r requirements.txt

if ! command -v g++ &> /dev/null; then
    exit 1
fi

mkdir -p bin

g++ -O3 -std=c++17 -Wall -o bin/tokenizer src/tokenizer.cpp
g++ -O3 -std=c++17 -Wall -o bin/indexer src/indexer.cpp
g++ -O3 -std=c++17 -Wall -o bin/search src/search.cpp
