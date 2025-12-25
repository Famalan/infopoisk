#include <iostream>
#include <string>
#include <fstream>
#include <algorithm>

#include "common.hpp"
#include "hash_table.hpp"
#include "compression.hpp"
#include "tokenizer_lib.hpp"

struct TermEntry
{
    uint64_t offset;
    uint32_t doc_count;
};

struct DocInfo
{
    std::string url;
    std::string title;
};

HashMap<TermEntry> term_dict;
SimpleVector<DocInfo> docs;
std::string postings_data;

SimpleVector<int> set_union(const SimpleVector<int> &a, const SimpleVector<int> &b)
{
    SimpleVector<int> res;
    size_t i = 0, j = 0;
    while (i < a.size && j < b.size)
    {
        if (a[i] < b[j])
        {
            res.push_back(a[i]);
            i++;
        }
        else if (b[j] < a[i])
        {
            res.push_back(b[j]);
            j++;
        }
        else
        {
            res.push_back(a[i]);
            i++;
            j++;
        }
    }
    while (i < a.size)
        res.push_back(a[i++]);
    while (j < b.size)
        res.push_back(b[j++]);
    return res;
}

SimpleVector<int> set_intersect(const SimpleVector<int> &a, const SimpleVector<int> &b)
{
    SimpleVector<int> res;
    size_t i = 0, j = 0;
    while (i < a.size && j < b.size)
    {
        if (a[i] < b[j])
            i++;
        else if (b[j] < a[i])
            j++;
        else
        {
            res.push_back(a[i]);
            i++;
            j++;
        }
    }
    return res;
}

SimpleVector<int> set_diff(const SimpleVector<int> &a, const SimpleVector<int> &b)
{
    SimpleVector<int> res;
    size_t i = 0, j = 0;
    while (i < a.size && j < b.size)
    {
        if (a[i] < b[j])
        {
            res.push_back(a[i]);
            i++;
        }
        else if (b[j] < a[i])
            j++;
        else
        {
            i++;
            j++;
        }
    }
    while (i < a.size)
        res.push_back(a[i++]);
    return res;
}

void load_index(const std::string &index_dir)
{
    std::string path_docs = index_dir + "/index.docs";
    std::string path_dict = index_dir + "/index.dict";
    std::string path_post = index_dir + "/index.postings";

    std::ifstream f_docs(path_docs, std::ios::binary);
    if (!f_docs)
        throw std::runtime_error("Cannot open docs");

    char magic[4];
    uint16_t ver;
    uint32_t count;
    f_docs.read(magic, 4);
    f_docs.read((char *)&ver, 2);
    f_docs.read((char *)&count, 4);

    f_docs.seekg(count * 8, std::ios::cur);

    for (size_t i = 0; i < count; ++i)
    {
        uint16_t len;
        DocInfo d;

        f_docs.read((char *)&len, 2);
        d.url.resize(len);
        f_docs.read(&d.url[0], len);

        f_docs.read((char *)&len, 2);
        d.title.resize(len);
        f_docs.read(&d.title[0], len);

        docs.push_back(d);
    }
    f_docs.close();

    std::ifstream f_dict(path_dict, std::ios::binary);
    if (!f_dict)
        throw std::runtime_error("Cannot open dict");

    f_dict.read(magic, 4);
    f_dict.read((char *)&ver, 2);
    f_dict.read((char *)&count, 4);

    term_dict.reserve(count);

    for (size_t i = 0; i < count; ++i)
    {
        uint8_t len;
        f_dict.read((char *)&len, 1);
        std::string term;
        term.resize(len);
        f_dict.read(&term[0], len);

        TermEntry e;
        f_dict.read((char *)&e.offset, 8);
        f_dict.read((char *)&e.doc_count, 4);

        term_dict.insert(std::move(term), std::move(e));
        if (i % 500000 == 0)
        {
            std::cerr << "Loaded " << i << " terms...\r";
        }
    }
    std::cerr << "Loaded " << count << " terms." << std::endl;
    f_dict.close();

    std::ifstream f_post(path_post, std::ios::binary | std::ios::ate);
    if (!f_post)
        throw std::runtime_error("Cannot open postings");
    std::streamsize size = f_post.tellg();
    f_post.seekg(0, std::ios::beg);

    postings_data.resize(size);
    f_post.read(&postings_data[0], size);
    f_post.close();

    std::cerr << "Loaded " << docs.size << " docs and " << count << " terms." << std::endl;
}

struct DocPositions
{
    int doc_id;
    SimpleVector<int> positions;
};

SimpleVector<int> get_postings(const std::string &term)
{
    SimpleVector<int> res;
    TermEntry *e = term_dict.get(term);
    if (!e)
        return res;

    const uint8_t *ptr = (const uint8_t *)postings_data.data() + e->offset;
    size_t offset = 0;

    auto p1 = Compression::decode_varbyte(ptr, offset);
    uint32_t doc_freq = p1.first;
    offset = p1.second;

    int curr_doc = 0;
    for (size_t i = 0; i < doc_freq; ++i)
    {
        auto p2 = Compression::decode_varbyte(ptr, offset);
        curr_doc += p2.first;
        offset = p2.second;

        res.push_back(curr_doc);

        auto p3 = Compression::decode_varbyte(ptr, offset);
        uint32_t freq = p3.first;
        offset = p3.second;

        for (size_t j = 0; j < freq; ++j)
        {
            auto p4 = Compression::decode_varbyte(ptr, offset);
            offset = p4.second;
        }
    }
    return res;
}

SimpleVector<DocPositions> get_full_postings(const std::string &term)
{
    SimpleVector<DocPositions> res;
    TermEntry *e = term_dict.get(term);
    if (!e)
        return res;

    const uint8_t *ptr = (const uint8_t *)postings_data.data() + e->offset;
    size_t offset = 0;

    auto p1 = Compression::decode_varbyte(ptr, offset);
    uint32_t doc_freq = p1.first;
    offset = p1.second;

    int curr_doc = 0;
    for (size_t i = 0; i < doc_freq; ++i)
    {
        auto p2 = Compression::decode_varbyte(ptr, offset);
        curr_doc += p2.first;
        offset = p2.second;

        DocPositions dp;
        dp.doc_id = curr_doc;

        auto p3 = Compression::decode_varbyte(ptr, offset);
        uint32_t freq = p3.first;
        offset = p3.second;

        int curr_pos = 0;
        for (size_t j = 0; j < freq; ++j)
        {
            auto p4 = Compression::decode_varbyte(ptr, offset);
            curr_pos += p4.first;
            offset = p4.second;
            dp.positions.push_back(curr_pos);
        }
        res.push_back(dp);
    }
    return res;
}

bool find_path(SimpleVector<int> *pos_lists, int count, int idx, int prev_pos, int first_pos, int max_dist, bool exact)
{
    if (idx == count)
        return true;

    SimpleVector<int> &candidates = pos_lists[idx];
    for (size_t i = 0; i < candidates.size; ++i)
    {
        int pos = candidates[i];
        if (idx == 0)
        {
            if (find_path(pos_lists, count, idx + 1, pos, pos, max_dist, exact))
                return true;
        }
        else
        {
            if (pos > prev_pos)
            {
                if (exact && pos != prev_pos + 1)
                    continue;
                if ((pos - first_pos) > max_dist)
                    continue;
                if (find_path(pos_lists, count, idx + 1, pos, first_pos, max_dist, exact))
                    return true;
            }
        }
    }
    return false;
}

SimpleVector<int> sequence_search(SimpleVector<std::string> &terms, int max_dist)
{
    if (terms.size == 0)
        return SimpleVector<int>();

    SimpleVector<int> docs_intersection = get_postings(terms[0]);
    for (size_t i = 1; i < terms.size; ++i)
    {
        docs_intersection = set_intersect(docs_intersection, get_postings(terms[i]));
    }

    if (docs_intersection.size == 0)
        return docs_intersection;

    SimpleVector<int> result;

    SimpleVector<SimpleVector<DocPositions>> all_term_postings;
    for (size_t i = 0; i < terms.size; ++i)
    {
        all_term_postings.push_back(get_full_postings(terms[i]));
    }

    for (size_t k = 0; k < docs_intersection.size; ++k)
    {
        int doc_id = docs_intersection[k];

        SimpleVector<int> *pos_lists = new SimpleVector<int>[terms.size];
        bool found_all = true;

        for (size_t t = 0; t < terms.size; ++t)
        {
            bool found_term_doc = false;
            for (size_t p = 0; p < all_term_postings[t].size; ++p)
            {
                if (all_term_postings[t][p].doc_id == doc_id)
                {
                    pos_lists[t] = all_term_postings[t][p].positions;
                    found_term_doc = true;
                    break;
                }
            }
            if (!found_term_doc)
            {
                found_all = false;
                break;
            }
        }

        if (found_all)
        {
            bool exact = (max_dist == (int)terms.size);
            if (find_path(pos_lists, terms.size, 0, -1, -1, max_dist, exact))
            {
                result.push_back(doc_id);
            }
        }

        delete[] pos_lists;
    }

    return result;
}

// Токены для парсера
enum TokenType
{
    TOK_TERM,
    TOK_AND,
    TOK_OR,
    TOK_NOT,
    TOK_LPAREN,
    TOK_RPAREN,
    TOK_END
};

struct Token
{
    TokenType type;
    std::string value;
};

SimpleVector<Token> tokenize_query(const std::string &query)
{
    SimpleVector<Token> tokens;
    size_t i = 0;
    while (i < query.size())
    {
        while (i < query.size() && query[i] == ' ')
            i++;
        if (i >= query.size())
            break;

        if (query[i] == '(')
        {
            tokens.push_back({TOK_LPAREN, "("});
            i++;
        }
        else if (query[i] == ')')
        {
            tokens.push_back({TOK_RPAREN, ")"});
            i++;
        }
        else if (query[i] == '!' && (i + 1 >= query.size() || query[i + 1] != '='))
        {
            tokens.push_back({TOK_NOT, "!"});
            i++;
        }
        else if (i + 1 < query.size() && query[i] == '&' && query[i + 1] == '&')
        {
            tokens.push_back({TOK_AND, "&&"});
            i += 2;
        }
        else if (i + 1 < query.size() && query[i] == '|' && query[i + 1] == '|')
        {
            tokens.push_back({TOK_OR, "||"});
            i += 2;
        }
        else if (std::isalnum((unsigned char)query[i]))
        {
            std::string term;
            while (i < query.size() && std::isalnum((unsigned char)query[i]))
            {
                term += std::tolower((unsigned char)query[i]);
                i++;
            }
            term = TokenizerLib::stem(term);
            tokens.push_back({TOK_TERM, term});
        }
        else
        {
            i++;
        }
    }
    tokens.push_back({TOK_END, ""});
    return tokens;
}

// Рекурсивный спуск парсер
class BoolParser
{
    SimpleVector<Token> tokens;
    size_t pos;

    Token &current() { return tokens[pos]; }
    void advance()
    {
        if (pos < tokens.size - 1)
            pos++;
    }

    SimpleVector<int> all_docs()
    {
        SimpleVector<int> res;
        for (size_t i = 0; i < docs.size; i++)
            res.push_back(i);
        return res;
    }

    // factor = TERM | NOT factor | LPAREN expr RPAREN
    SimpleVector<int> parse_factor()
    {
        if (current().type == TOK_NOT)
        {
            advance();
            SimpleVector<int> operand = parse_factor();
            return set_diff(all_docs(), operand);
        }
        if (current().type == TOK_LPAREN)
        {
            advance();
            SimpleVector<int> res = parse_or();
            if (current().type == TOK_RPAREN)
                advance();
            return res;
        }
        if (current().type == TOK_TERM)
        {
            std::string term = current().value;
            advance();
            return get_postings(term);
        }
        return SimpleVector<int>();
    }

    // term = factor ((AND | implicit) factor)*
    SimpleVector<int> parse_and()
    {
        SimpleVector<int> left = parse_factor();
        while (current().type == TOK_AND || current().type == TOK_TERM ||
               current().type == TOK_NOT || current().type == TOK_LPAREN)
        {
            if (current().type == TOK_AND)
                advance();
            SimpleVector<int> right = parse_factor();
            left = set_intersect(left, right);
        }
        return left;
    }

    // expr = term (OR term)*
    SimpleVector<int> parse_or()
    {
        SimpleVector<int> left = parse_and();
        while (current().type == TOK_OR)
        {
            advance();
            SimpleVector<int> right = parse_and();
            left = set_union(left, right);
        }
        return left;
    }

public:
    SimpleVector<int> parse(const std::string &query)
    {
        tokens = tokenize_query(query);
        pos = 0;
        if (tokens.size <= 1)
            return SimpleVector<int>();
        return parse_or();
    }
};

SimpleVector<int> evaluate(const std::string &query)
{
    BoolParser parser;
    return parser.parse(query);
}

int main(int argc, char *argv[])
{
    std::setvbuf(stdout, NULL, _IOLBF, 0);
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    if (argc < 2)
    {
        std::cerr << "Usage: search <index_dir>" << std::endl;
        return 1;
    }
    std::string index_dir = argv[1];

    std::cerr << "Starting Search Engine..." << std::endl;

    try
    {
        load_index(index_dir);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error loading index: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "Ready" << std::endl;
    std::cerr << "Index loaded. Ready for queries." << std::endl;

    std::string line;
    while (std::getline(std::cin, line))
    {
        if (line == "exit")
            break;
        if (line.empty())
            continue;

        SimpleVector<int> results = evaluate(line);

        std::cout << "Found " << results.size << " docs." << std::endl;
        for (size_t i = 0; i < results.size && i < 50; ++i)
        {
            int id = results[i];
            if (id < (int)docs.size)
            {
                std::cout << docs[id].title << " (" << docs[id].url << ")" << std::endl;
            }
        }
        std::cout << "__END_QUERY__" << std::endl;
    }

    return 0;
}
