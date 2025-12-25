#ifndef COMMON_HPP
#define COMMON_HPP

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>

template <typename T>
class SimpleVector {
public:
    T* data;
    size_t size;
    size_t capacity;

    SimpleVector() : data(nullptr), size(0), capacity(0) {}

    SimpleVector(const SimpleVector& other) : data(nullptr), size(0), capacity(0) {
        if (other.size > 0) {
            capacity = other.size;
            data = (T*)std::malloc(capacity * sizeof(T));
            if (!data) throw std::runtime_error("Memory allocation failed");
            for (size_t i = 0; i < other.size; ++i) {
                new (data + i) T(other.data[i]);
            }
            size = other.size;
        }
    }

    SimpleVector(SimpleVector&& other) noexcept : data(other.data), size(other.size), capacity(other.capacity) {
        other.data = nullptr;
        other.size = 0;
        other.capacity = 0;
    }

    SimpleVector& operator=(const SimpleVector& other) {
        if (this != &other) {
            clear();
            if (other.size > 0) {
                capacity = other.size;
                data = (T*)std::malloc(capacity * sizeof(T));
                if (!data) throw std::runtime_error("Memory allocation failed");
                for (size_t i = 0; i < other.size; ++i) {
                    new (data + i) T(other.data[i]);
                }
                size = other.size;
            }
        }
        return *this;
    }

    SimpleVector& operator=(SimpleVector&& other) noexcept {
        if (this != &other) {
            clear();
            data = other.data;
            size = other.size;
            capacity = other.capacity;
            other.data = nullptr;
            other.size = 0;
            other.capacity = 0;
        }
        return *this;
    }

    ~SimpleVector() {
        clear();
    }

    void push_back(const T& value) {
        if (size == capacity) {
            reallocate((capacity == 0) ? 8 : capacity * 2);
        }
        new (data + size) T(value);
        size++;
    }

    void push_back(T&& value) {
        if (size == capacity) {
            reallocate((capacity == 0) ? 8 : capacity * 2);
        }
        new (data + size) T(std::move(value));
        size++;
    }

    T& operator[](size_t index) {
        if (index >= size) throw std::out_of_range("Index out of bounds");
        return data[index];
    }

    const T& operator[](size_t index) const {
        if (index >= size) throw std::out_of_range("Index out of bounds");
        return data[index];
    }

    void clear() {
        if (data) {
            for(size_t i=0; i<size; ++i) {
                data[i].~T();
            }
            std::free(data);
            data = nullptr;
        }
        size = 0;
        capacity = 0;
    }
    
    T* begin() { return data; }
    T* end() { return data + size; }

private:
    void reallocate(size_t new_capacity) {
        T* new_data = (T*)std::malloc(new_capacity * sizeof(T));
        if (!new_data) throw std::runtime_error("Memory allocation failed");
        
        if (data) {
            for(size_t i=0; i<size; ++i) {
                new (new_data + i) T(std::move(data[i]));
                data[i].~T();
            }
            std::free(data);
        }
        data = new_data;
        capacity = new_capacity;
    }
};

#endif
