#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <algorithm>

#include "common.hpp"
#include "hash_table.hpp"
#include "tokenizer_lib.hpp"
#include "compression.hpp"

struct TermPostings
{
  struct DocEntry
  {
    int doc_id;
    SimpleVector<int> positions;
  };

  SimpleVector<DocEntry> doc_entries;

  void add_position(int doc_id, int pos)
  {
    if (doc_entries.size == 0 || doc_entries[doc_entries.size - 1].doc_id != doc_id)
    {
      DocEntry entry;
      entry.doc_id = doc_id;
      entry.positions.push_back(pos);
      doc_entries.push_back(std::move(entry));
    }
    else
    {
      doc_entries[doc_entries.size - 1].positions.push_back(pos);
    }
  }
};

HashMap<TermPostings> index_map;
SimpleVector<std::string> doc_urls;
SimpleVector<std::string> doc_titles;

const char MAGIC_DOCS[] = "DOCS";
const char MAGIC_DICT[] = "DICT";
const char MAGIC_POST[] = "POST";
const uint16_t VERSION = 3;

void write_index(const std::string &out_dir)
{
  std::cerr << "Writing index to " << out_dir << "..." << std::endl;

  std::string path_docs = out_dir + "/index.docs";
  std::string path_dict = out_dir + "/index.dict";
  std::string path_post = out_dir + "/index.postings";

  std::ofstream f_docs(path_docs, std::ios::binary);
  f_docs.write(MAGIC_DOCS, 4);
  f_docs.write((char *)&VERSION, 2);
  uint32_t doc_count = doc_urls.size;
  f_docs.write((char *)&doc_count, 4);

  SimpleVector<uint64_t> offsets;
  uint64_t current_offset = 4 + 2 + 4 + (doc_count * 8);

  for (size_t i = 0; i < doc_count; ++i)
  {
    offsets.push_back(current_offset);
    uint16_t url_len = doc_urls[i].size();
    uint16_t title_len = doc_titles[i].size();
    current_offset += 2 + url_len + 2 + title_len;
  }

  for (size_t i = 0; i < doc_count; ++i)
  {
    f_docs.write((char *)&offsets[i], 8);
  }

  for (size_t i = 0; i < doc_count; ++i)
  {
    uint16_t url_len = doc_urls[i].size();
    f_docs.write((char *)&url_len, 2);
    f_docs.write(doc_urls[i].c_str(), url_len);

    uint16_t title_len = doc_titles[i].size();
    f_docs.write((char *)&title_len, 2);
    f_docs.write(doc_titles[i].c_str(), title_len);
  }
  f_docs.close();

  std::ofstream f_dict(path_dict, std::ios::binary);
  std::ofstream f_post(path_post, std::ios::binary);

  f_dict.write(MAGIC_DICT, 4);
  f_dict.write((char *)&VERSION, 2);
  uint32_t term_count = 0;
  long term_count_pos = f_dict.tellp();
  f_dict.write((char *)&term_count, 4);

  f_post.write(MAGIC_POST, 4);
  f_post.write((char *)&VERSION, 2);

  for (auto it = index_map.begin(); it != index_map.end(); ++it)
  {
    term_count++;
    std::string term = it->key;
    TermPostings &postings = it->value;

    uint64_t post_offset = f_post.tellp();
    uint32_t doc_freq = postings.doc_entries.size;

    uint8_t term_len = std::min((size_t)255, term.size());
    f_dict.write((char *)&term_len, 1);
    f_dict.write(term.c_str(), term_len);
    f_dict.write((char *)&post_offset, 8);
    f_dict.write((char *)&doc_freq, 4);

    SimpleVector<uint8_t> compressed;
    Compression::encode_varbyte(doc_freq, compressed);

    int prev_doc_id = 0;
    for (size_t i = 0; i < doc_freq; ++i)
    {
      TermPostings::DocEntry &entry = postings.doc_entries[i];
      Compression::encode_varbyte(entry.doc_id - prev_doc_id, compressed);
      prev_doc_id = entry.doc_id;

      uint32_t freq = entry.positions.size;
      Compression::encode_varbyte(freq, compressed);

      int prev_pos = 0;
      for (size_t j = 0; j < freq; ++j)
      {
        Compression::encode_varbyte(entry.positions[j] - prev_pos, compressed);
        prev_pos = entry.positions[j];
      }
    }
    f_post.write((char *)compressed.data, compressed.size);
  }

  f_dict.seekp(term_count_pos);
  f_dict.write((char *)&term_count, 4);

  f_dict.close();
  f_post.close();

  std::cerr << "Indexing complete. Terms: " << term_count << ", Docs: " << doc_count << std::endl;
}

int main(int argc, char *argv[])
{
  if (argc < 2)
  {
    std::cerr << "Usage: indexer <out_dir>" << std::endl;
    return 1;
  }
  std::string out_dir = argv[1];

  std::string line;
  int doc_id = 0;
  SimpleVector<std::string> tokens;

  while (std::getline(std::cin, line))
  {
    if (line.empty())
      continue;

    size_t tab1 = line.find('\t');
    if (tab1 == std::string::npos)
      continue;

    size_t tab2 = line.find('\t', tab1 + 1);
    if (tab2 == std::string::npos)
      continue;

    doc_urls.push_back(line.substr(0, tab1));
    doc_titles.push_back(line.substr(tab1 + 1, tab2 - tab1 - 1));

    std::string text = line.substr(tab2 + 1);
    tokens.clear();
    TokenizerLib::tokenize(text, tokens);

    for (size_t i = 0; i < tokens.size; ++i)
    {
      index_map[tokens[i]].add_position(doc_id, (int)i);
    }

    doc_id++;
    if (doc_id % 100 == 0)
    {
      std::cerr << "Processed " << doc_id << " docs...\r";
    }
  }
  std::cerr << std::endl;

  write_index(out_dir);
  return 0;
}
