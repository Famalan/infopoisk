#ifndef COMPRESSION_HPP
#define COMPRESSION_HPP

#include "common.hpp"

namespace Compression {

void encode_varbyte(uint32_t number, SimpleVector<uint8_t>& out) {
    while (number >= 128) {
        out.push_back((number & 0x7F) | 0x80);
        number >>= 7;
    }
    out.push_back(number & 0x7F);
}

std::pair<uint32_t, size_t> decode_varbyte(const uint8_t* data, size_t offset) {
    uint32_t value = 0;
    int shift = 0;
    while (true) {
        uint8_t byte = data[offset++];
        value |= (byte & 0x7F) << shift;
        if (!(byte & 0x80)) break;
        shift += 7;
    }
    return {value, offset};
}

void encode_delta_varbyte(const SimpleVector<int>& values, SimpleVector<uint8_t>& out) {
    int prev = 0;
    for (size_t i = 0; i < values.size; ++i) {
        int delta = values[i] - prev;
        encode_varbyte((uint32_t)delta, out);
        prev = values[i];
    }
}

}

#endif
