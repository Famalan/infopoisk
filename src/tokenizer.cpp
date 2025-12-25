#include <cctype>
#include <iostream>
#include <string>

namespace
{

    bool is_consonant(const std::string &w, int i)
    {
        const char ch = w[static_cast<size_t>(i)];
        switch (ch)
        {
        case 'a':
        case 'e':
        case 'i':
        case 'o':
        case 'u':
            return false;
        case 'y':
            if (i == 0)
            {
                return true;
            }
            return !is_consonant(w, i - 1);
        default:
            return true;
        }
    }

    int measure(const std::string &w)
    {
        const int len = static_cast<int>(w.size());
        int n = 0;
        int i = 0;

        while (i < len && is_consonant(w, i))
        {
            i++;
        }

        while (i < len)
        {
            while (i < len && !is_consonant(w, i))
            {
                i++;
            }
            if (i >= len)
            {
                break;
            }

            while (i < len && is_consonant(w, i))
            {
                i++;
            }
            n++;
        }

        return n;
    }

    bool contains_vowel(const std::string &w)
    {
        for (int i = 0; i < static_cast<int>(w.size()); i++)
        {
            if (!is_consonant(w, i))
            {
                return true;
            }
        }
        return false;
    }

    bool ends_with(const std::string &w, const std::string &suffix)
    {
        if (w.size() < suffix.size())
        {
            return false;
        }
        return w.compare(w.size() - suffix.size(), suffix.size(), suffix) == 0;
    }

    std::string stem_part(const std::string &w, const std::string &suffix)
    {
        return w.substr(0, w.size() - suffix.size());
    }

    bool double_consonant(const std::string &w)
    {
        if (w.size() < 2)
        {
            return false;
        }
        const size_t n = w.size();
        if (w[n - 1] != w[n - 2])
        {
            return false;
        }
        return is_consonant(w, static_cast<int>(n - 1));
    }

    bool cvc(const std::string &w)
    {
        if (w.size() < 3)
        {
            return false;
        }
        const int i = static_cast<int>(w.size() - 1);
        if (!is_consonant(w, i))
        {
            return false;
        }
        if (is_consonant(w, i - 1))
        {
            return false;
        }
        if (!is_consonant(w, i - 2))
        {
            return false;
        }
        const char last = w.back();
        if (last == 'w' || last == 'x' || last == 'y')
        {
            return false;
        }
        return true;
    }

    void replace_suffix(std::string &w, const std::string &suffix, const std::string &replacement)
    {
        w.erase(w.size() - suffix.size());
        w += replacement;
    }

    void step1a(std::string &w)
    {
        if (ends_with(w, "sses"))
        {
            replace_suffix(w, "sses", "ss");
            return;
        }
        if (ends_with(w, "ies"))
        {
            replace_suffix(w, "ies", "i");
            return;
        }
        if (ends_with(w, "ss"))
        {
            return;
        }
        if (ends_with(w, "s"))
        {
            w.pop_back();
        }
    }

    void step1b(std::string &w)
    {
        if (ends_with(w, "eed"))
        {
            std::string stem = stem_part(w, "eed");
            if (measure(stem) > 0)
            {
                replace_suffix(w, "eed", "ee");
            }
            return;
        }

        bool removed = false;
        std::string stem;

        if (ends_with(w, "ed"))
        {
            stem = stem_part(w, "ed");
            if (contains_vowel(stem))
            {
                w = stem;
                removed = true;
            }
        }
        else if (ends_with(w, "ing"))
        {
            stem = stem_part(w, "ing");
            if (contains_vowel(stem))
            {
                w = stem;
                removed = true;
            }
        }

        if (!removed)
        {
            return;
        }

        if (ends_with(w, "at") || ends_with(w, "bl") || ends_with(w, "iz"))
        {
            w += "e";
            return;
        }

        if (double_consonant(w))
        {
            const char last = w.back();
            if (last != 'l' && last != 's' && last != 'z')
            {
                w.pop_back();
            }
            return;
        }

        if (measure(w) == 1 && cvc(w))
        {
            w += "e";
        }
    }

    void step1c(std::string &w)
    {
        if (!ends_with(w, "y"))
        {
            return;
        }
        std::string stem = stem_part(w, "y");
        if (contains_vowel(stem))
        {
            w[w.size() - 1] = 'i';
        }
    }

    struct Rule
    {
        const char *suffix;
        const char *replacement;
    };

    void step2(std::string &w)
    {
        static const Rule rules[] = {
            {"ational", "ate"},
            {"tional", "tion"},
            {"enci", "ence"},
            {"anci", "ance"},
            {"izer", "ize"},
            {"abli", "able"},
            {"alli", "al"},
            {"entli", "ent"},
            {"eli", "e"},
            {"ousli", "ous"},
            {"ization", "ize"},
            {"ation", "ate"},
            {"ator", "ate"},
            {"alism", "al"},
            {"iveness", "ive"},
            {"fulness", "ful"},
            {"ousness", "ous"},
            {"aliti", "al"},
            {"iviti", "ive"},
            {"biliti", "ble"},
        };

        for (const auto &r : rules)
        {
            const std::string suf(r.suffix);
            if (!ends_with(w, suf))
            {
                continue;
            }
            const std::string stem = stem_part(w, suf);
            if (measure(stem) > 0)
            {
                replace_suffix(w, suf, r.replacement);
            }
            return;
        }
    }

    void step3(std::string &w)
    {
        static const Rule rules[] = {
            {"icate", "ic"},
            {"ative", ""},
            {"alize", "al"},
            {"iciti", "ic"},
            {"ical", "ic"},
            {"ful", ""},
            {"ness", ""},
        };

        for (const auto &r : rules)
        {
            const std::string suf(r.suffix);
            if (!ends_with(w, suf))
            {
                continue;
            }
            const std::string stem = stem_part(w, suf);
            if (measure(stem) > 0)
            {
                replace_suffix(w, suf, r.replacement);
            }
            return;
        }
    }

    void step4(std::string &w)
    {
        static const char *suffixes[] = {
            "al",
            "ance",
            "ence",
            "er",
            "ic",
            "able",
            "ible",
            "ant",
            "ement",
            "ment",
            "ent",
            "ou",
            "ism",
            "ate",
            "iti",
            "ous",
            "ive",
            "ize",
        };

        for (const char *s : suffixes)
        {
            const std::string suf(s);
            if (!ends_with(w, suf))
            {
                continue;
            }
            const std::string stem = stem_part(w, suf);
            if (measure(stem) > 1)
            {
                w = stem;
            }
            return;
        }

        if (ends_with(w, "ion"))
        {
            const std::string stem = stem_part(w, "ion");
            if (stem.size() >= 1)
            {
                const char prev = stem.back();
                if ((prev == 's' || prev == 't') && measure(stem) > 1)
                {
                    w = stem;
                }
            }
        }
    }

    void step5a(std::string &w)
    {
        if (!ends_with(w, "e"))
        {
            return;
        }
        const std::string stem = stem_part(w, "e");
        const int m = measure(stem);
        if (m > 1)
        {
            w = stem;
            return;
        }
        if (m == 1 && !cvc(stem))
        {
            w = stem;
        }
    }

    void step5b(std::string &w)
    {
        if (measure(w) > 1 && ends_with(w, "ll"))
        {
            w.pop_back();
        }
    }

    std::string porter_stem(std::string w)
    {
        if (w.size() <= 2)
        {
            return w;
        }

        step1a(w);
        step1b(w);
        step1c(w);
        step2(w);
        step3(w);
        step4(w);
        step5a(w);
        step5b(w);
        return w;
    }

    bool is_token_char(unsigned char ch)
    {
        return std::isalnum(ch) != 0;
    }

    void flush_token(std::string &token)
    {
        if (token.empty())
        {
            return;
        }
        std::string out = porter_stem(token);
        if (!out.empty())
        {
            std::cout << out << "\n";
        }
        token.clear();
    }

}

int main()
{
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    std::string line;
    while (std::getline(std::cin, line))
    {
        std::string token;
        for (char ch : line)
        {
            const unsigned char uch = static_cast<unsigned char>(ch);
            if (is_token_char(uch))
            {
                token.push_back(static_cast<char>(std::tolower(uch)));
            }
            else
            {
                flush_token(token);
            }
        }
        flush_token(token);
        std::cout << "__END_DOC__" << std::endl;
    }
    return 0;
}
