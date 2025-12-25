#ifndef TOKENIZER_LIB_HPP
#define TOKENIZER_LIB_HPP

#include <string>
#include <cctype>
#include "common.hpp"

namespace TokenizerLib {

bool is_consonant(const std::string& w, int i) {
    const char ch = w[static_cast<size_t>(i)];
    switch (ch) {
        case 'a': case 'e': case 'i': case 'o': case 'u': return false;
        case 'y': return (i == 0) ? true : !is_consonant(w, i - 1);
        default: return true;
    }
}

int measure(const std::string& w) {
    const int len = static_cast<int>(w.size());
    int n = 0;
    int i = 0;
    while (i < len && is_consonant(w, i)) i++;
    while (i < len) {
        while (i < len && !is_consonant(w, i)) i++;
        if (i >= len) break;
        while (i < len && is_consonant(w, i)) i++;
        n++;
    }
    return n;
}

bool contains_vowel(const std::string& w) {
    for (size_t i = 0; i < w.size(); i++) if (!is_consonant(w, i)) return true;
    return false;
}

bool ends_with(const std::string& w, const std::string& suffix) {
    if (w.size() < suffix.size()) return false;
    return w.compare(w.size() - suffix.size(), suffix.size(), suffix) == 0;
}

std::string stem_part(const std::string& w, const std::string& suffix) {
    return w.substr(0, w.size() - suffix.size());
}

bool double_consonant(const std::string& w) {
    if (w.size() < 2) return false;
    if (w.back() != w[w.size() - 2]) return false;
    return is_consonant(w, w.size() - 1);
}

bool cvc(const std::string& w) {
    if (w.size() < 3) return false;
    if (!is_consonant(w, w.size() - 1) || is_consonant(w, w.size() - 2) || !is_consonant(w, w.size() - 3)) return false;
    const char last = w.back();
    return !(last == 'w' || last == 'x' || last == 'y');
}

void replace_suffix(std::string& w, const std::string& suffix, const std::string& replacement) {
    w.erase(w.size() - suffix.size());
    w += replacement;
}

void step1a(std::string& w) {
    if (ends_with(w, "sses")) replace_suffix(w, "sses", "ss");
    else if (ends_with(w, "ies")) replace_suffix(w, "ies", "i");
    else if (ends_with(w, "ss")) return;
    else if (ends_with(w, "s")) w.pop_back();
}

void step1b(std::string& w) {
    if (ends_with(w, "eed")) {
        std::string stem = stem_part(w, "eed");
        if (measure(stem) > 0) replace_suffix(w, "eed", "ee");
        return;
    }
    bool removed = false;
    std::string stem;
    if (ends_with(w, "ed")) {
        stem = stem_part(w, "ed");
        if (contains_vowel(stem)) { w = stem; removed = true; }
    } else if (ends_with(w, "ing")) {
        stem = stem_part(w, "ing");
        if (contains_vowel(stem)) { w = stem; removed = true; }
    }
    if (removed) {
        if (ends_with(w, "at") || ends_with(w, "bl") || ends_with(w, "iz")) w += "e";
        else if (double_consonant(w)) {
            const char last = w.back();
            if (last != 'l' && last != 's' && last != 'z') w.pop_back();
        } else if (measure(w) == 1 && cvc(w)) w += "e";
    }
}

void step1c(std::string& w) {
    if (ends_with(w, "y")) {
        std::string stem = stem_part(w, "y");
        if (contains_vowel(stem)) w.back() = 'i';
    }
}

void step2(std::string& w) {
    static const struct { const char* s; const char* r; } rules[] = {
        {"ational", "ate"}, {"tional", "tion"}, {"enci", "ence"}, {"anci", "ance"},
        {"izer", "ize"}, {"abli", "able"}, {"alli", "al"}, {"entli", "ent"},
        {"eli", "e"}, {"ousli", "ous"}, {"ization", "ize"}, {"ation", "ate"},
        {"ator", "ate"}, {"alism", "al"}, {"iveness", "ive"}, {"fulness", "ful"},
        {"ousness", "ous"}, {"aliti", "al"}, {"iviti", "ive"}, {"biliti", "ble"}
    };
    for (const auto& rule : rules) {
        if (ends_with(w, rule.s)) {
            std::string stem = stem_part(w, rule.s);
            if (measure(stem) > 0) replace_suffix(w, rule.s, rule.r);
            return;
        }
    }
}

void step3(std::string& w) {
    static const struct { const char* s; const char* r; } rules[] = {
        {"icate", "ic"}, {"ative", ""}, {"alize", "al"}, {"iciti", "ic"},
        {"ical", "ic"}, {"ful", ""}, {"ness", ""}
    };
    for (const auto& rule : rules) {
        if (ends_with(w, rule.s)) {
            std::string stem = stem_part(w, rule.s);
            if (measure(stem) > 0) replace_suffix(w, rule.s, rule.r);
            return;
        }
    }
}

void step4(std::string& w) {
    static const char* suffixes[] = {
        "al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent",
        "ou", "ism", "ate", "iti", "ous", "ive", "ize"
    };
    for (const char* s : suffixes) {
        if (ends_with(w, s)) {
            std::string stem = stem_part(w, s);
            if (measure(stem) > 1) w = stem;
            return;
        }
    }
    if (ends_with(w, "ion")) {
        std::string stem = stem_part(w, "ion");
        if (stem.size() >= 1) {
            char prev = stem.back();
            if ((prev == 's' || prev == 't') && measure(stem) > 1) w = stem;
        }
    }
}

void step5(std::string& w) {
    if (ends_with(w, "e")) {
        std::string stem = stem_part(w, "e");
        int m = measure(stem);
        if (m > 1 || (m == 1 && !cvc(stem))) w = stem;
    }
    if (measure(w) > 1 && ends_with(w, "ll")) w.pop_back();
}

std::string stem(std::string w) {
    if (w.size() <= 2) return w;
    step1a(w); step1b(w); step1c(w); step2(w); step3(w); step4(w); step5(w);
    return w;
}

void tokenize(const std::string& text, SimpleVector<std::string>& tokens) {
    std::string token;
    for (char ch : text) {
        if (std::isalnum((unsigned char)ch)) {
            token.push_back(std::tolower((unsigned char)ch));
        } else {
            if (!token.empty()) {
                tokens.push_back(stem(token));
                token.clear();
            }
        }
    }
    if (!token.empty()) {
        tokens.push_back(stem(token));
    }
}

}

#endif
