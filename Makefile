CXX = g++
CXXFLAGS = -O3 -std=c++17 -Wall

all: bin/tokenizer bin/indexer bin/search

bin/tokenizer: src/tokenizer.cpp
	mkdir -p bin
	$(CXX) $(CXXFLAGS) -o bin/tokenizer src/tokenizer.cpp

bin/indexer: src/indexer.cpp src/common.hpp src/hash_table.hpp src/compression.hpp src/tokenizer_lib.hpp
	mkdir -p bin
	$(CXX) $(CXXFLAGS) -o bin/indexer src/indexer.cpp

bin/search: src/search.cpp src/common.hpp src/hash_table.hpp src/compression.hpp src/tokenizer_lib.hpp
	mkdir -p bin
	$(CXX) $(CXXFLAGS) -o bin/search src/search.cpp

clean:
	rm -rf bin/
