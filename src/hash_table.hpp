#ifndef HASH_TABLE_HPP
#define HASH_TABLE_HPP

#include "common.hpp"
#include <string>
#include <utility>

template <typename T>
class HashMap
{
private:
    struct Entry
    {
        std::string key;
        T value;
        bool is_occupied = false;
        bool is_deleted = false;

        Entry() = default;
        ~Entry() = default;

        Entry(const Entry &) = delete;
        Entry &operator=(const Entry &) = delete;

        Entry(Entry &&other) noexcept : key(std::move(other.key)),
                                        value(std::move(other.value)),
                                        is_occupied(other.is_occupied),
                                        is_deleted(other.is_deleted)
        {
            other.is_occupied = false;
        }

        Entry &operator=(Entry &&other) noexcept
        {
            if (this != &other)
            {
                key = std::move(other.key);
                value = std::move(other.value);
                is_occupied = other.is_occupied;
                is_deleted = other.is_deleted;
                other.is_occupied = false;
            }
            return *this;
        }
    };

    Entry *table;
    size_t capacity;
    size_t count;

    size_t hash(const std::string &key) const
    {
        size_t h = 5381;
        for (char c : key)
        {
            h = ((h << 5) + h) + c;
        }
        return h;
    }

    void resize()
    {
        size_t old_capacity = capacity;
        Entry *old_table = table;

        capacity = (capacity == 0) ? 16 : capacity * 2;
        table = new Entry[capacity];
        count = 0;

        for (size_t i = 0; i < old_capacity; ++i)
        {
            if (old_table[i].is_occupied && !old_table[i].is_deleted)
            {
                insert_internal(std::move(old_table[i].key), std::move(old_table[i].value));
            }
        }

        delete[] old_table;
    }

    void insert_internal(std::string &&key, T &&value)
    {
        size_t idx = hash(key) % capacity;
        while (table[idx].is_occupied)
        {
            idx = (idx + 1) % capacity;
        }
        table[idx].key = std::move(key);
        table[idx].value = std::move(value);
        table[idx].is_occupied = true;
        table[idx].is_deleted = false;
        count++;
    }

public:
    HashMap() : table(nullptr), capacity(0), count(0) {}

    void reserve(size_t n)
    {
        if (n <= capacity * 0.7)
            return;

        size_t new_capacity = capacity == 0 ? 16 : capacity;
        while (new_capacity < n / 0.7)
            new_capacity *= 2;

        if (new_capacity <= capacity)
            return;

        size_t old_capacity = capacity;
        Entry *old_table = table;

        capacity = new_capacity;
        table = new Entry[capacity];
        count = 0;

        for (size_t i = 0; i < old_capacity; ++i)
        {
            if (old_table[i].is_occupied && !old_table[i].is_deleted)
            {
                insert_internal(std::move(old_table[i].key), std::move(old_table[i].value));
            }
        }

        if (old_table)
            delete[] old_table;
    }

    ~HashMap()
    {
        if (table)
            delete[] table;
    }

    void insert(std::string &&key, T &&value)
    {
        if (count >= capacity * 0.7)
        {
            resize();
        }

        size_t idx = hash(key) % capacity;
        while (table[idx].is_occupied)
        {
            if (table[idx].key == key && !table[idx].is_deleted)
            {
                table[idx].value = std::move(value);
                return;
            }
            idx = (idx + 1) % capacity;
        }

        table[idx].key = std::move(key);
        table[idx].value = std::move(value);
        table[idx].is_occupied = true;
        table[idx].is_deleted = false;
        count++;
    }

    void insert(const std::string &key, const T &value)
    {
        if (count >= capacity * 0.7)
        {
            resize();
        }

        size_t idx = hash(key) % capacity;
        while (table[idx].is_occupied)
        {
            if (table[idx].key == key && !table[idx].is_deleted)
            {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) % capacity;
        }

        table[idx].key = key;
        table[idx].value = value;
        table[idx].is_occupied = true;
        table[idx].is_deleted = false;
        count++;
    }

    void insert(const std::string &key, T &&value)
    {
        if (count >= capacity * 0.7)
        {
            resize();
        }

        size_t idx = hash(key) % capacity;
        while (table[idx].is_occupied)
        {
            if (table[idx].key == key && !table[idx].is_deleted)
            {
                table[idx].value = std::move(value);
                return;
            }
            idx = (idx + 1) % capacity;
        }

        table[idx].key = key;
        table[idx].value = std::move(value);
        table[idx].is_occupied = true;
        table[idx].is_deleted = false;
        count++;
    }

    T *get(const std::string &key)
    {
        if (capacity == 0)
            return nullptr;
        size_t idx = hash(key) % capacity;
        size_t start_idx = idx;

        while (table[idx].is_occupied)
        {
            if (table[idx].key == key && !table[idx].is_deleted)
            {
                return &table[idx].value;
            }
            idx = (idx + 1) % capacity;
            if (idx == start_idx)
                break;
        }
        return nullptr;
    }

    T &operator[](const std::string &key)
    {
        T *existing = get(key);
        if (existing)
            return *existing;

        if (count >= capacity * 0.7)
        {
            resize();
        }

        size_t idx = hash(key) % capacity;
        while (table[idx].is_occupied)
        {
            idx = (idx + 1) % capacity;
        }

        table[idx].key = key;
        table[idx].is_occupied = true;
        table[idx].is_deleted = false;
        count++;
        return table[idx].value;
    }

    class Iterator
    {
    public:
        Entry *ptr;
        Entry *end_ptr;
        Iterator(Entry *p, Entry *end) : ptr(p), end_ptr(end)
        {
            while (ptr < end_ptr && (!ptr->is_occupied || ptr->is_deleted))
                ptr++;
        }
        bool operator!=(const Iterator &other) { return ptr != other.ptr; }
        void operator++()
        {
            do
            {
                ptr++;
            } while (ptr < end_ptr && (!ptr->is_occupied || ptr->is_deleted));
        }
        Entry &operator*() { return *ptr; }
        Entry *operator->() { return ptr; }
    };

    Iterator begin() { return Iterator(table, table + capacity); }
    Iterator end() { return Iterator(table + capacity, table + capacity); }
};

#endif
