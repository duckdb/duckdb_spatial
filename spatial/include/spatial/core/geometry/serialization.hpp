#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// BinaryReader
//------------------------------------------------------------------------------
class BinaryReader {
private:
    data_ptr_t start;
    uint32_t size;
    uint32_t offset;
public:
    BinaryReader(const string_t &blob) {
        start = (data_ptr_t)blob.GetDataUnsafe();
        size = blob.GetSize();
        offset = 0;
    }
    
    void Reset() {
        offset = 0;
    }

    template<typename T>
    T Read() {
        if(offset + sizeof(T) > size) {
            throw SerializationException("Trying to read past end of buffer");
        }
        auto ptr = start + offset;
        offset += sizeof(T);
        return Load<T>(ptr);
    }

    template<typename T>
    T Peek() {
        if(offset + sizeof(T) > size) {
            throw SerializationException("Trying to read past end of buffer");
        }
        auto ptr = start + offset;
        return Load<T>(ptr);
    }

    void Skip(uint32_t bytes) {
        if(offset + bytes > size) {
            throw SerializationException("Trying to read past end of buffer");
        }
        offset += bytes;
    }

    data_ptr_t GetPtr() {
        return start + offset;
    }

    void SetPtr(data_ptr_t ptr) {
        if(ptr < start || ptr > start + size) {
            throw SerializationException("Trying to set ptr outside of buffer");
        }
        offset = ptr - start;
    }
};

class BinaryWriter {
private:
    data_ptr_t start;
    uint32_t size;
    uint32_t offset;
public:
    BinaryWriter(string_t &blob) {
        start = (data_ptr_t)blob.GetDataUnsafe();
        size = blob.GetSize();
        offset = 0;
    }

    void Reset() {
        offset = 0;
    }

    template<typename T>
    void Write(T value) {
        if(offset + sizeof(T) > size) {
            throw SerializationException("Trying to write past end of buffer, %d > %d", offset + sizeof(T), size);
        }
        auto ptr = start + offset;
        offset += sizeof(T);
        Store<T>(value, ptr);
    }

    void Skip(uint32_t bytes) {
        if(offset + bytes > size) {
            throw SerializationException("Trying to skip past end of buffer, %d > %d", offset + bytes, size);
        }
        offset += bytes;
    }
};

} // namespace core

} // namespace spatial