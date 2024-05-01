#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

// TODO: Split this into a read and write cursor. Get rid of bounds checks in release mode
class Cursor {
private:
	data_ptr_t start;
	data_ptr_t ptr;
	data_ptr_t end;

public:
	enum class Offset { START, CURRENT, END };

	explicit Cursor(data_ptr_t start, data_ptr_t end) : start(start), ptr(start), end(end) {
	}

	// Be really careful with passing string_ts here, if we accidentally copy we may end up writing to the inlined data
	// of a temporary
	explicit Cursor(const string_t &blob)
	    : start((data_ptr_t)blob.GetDataWriteable()), ptr(start), end(start + blob.GetSize()) {
	}

	data_ptr_t GetPtr() {
		return ptr;
	}

	uint32_t Remaining() {
		D_ASSERT(ptr <= end);
		return end - ptr;
	}

	void SetPtr(data_ptr_t ptr_p) {
		if (ptr_p < start || ptr_p > end) {
			throw SerializationException("Trying to set ptr outside of buffer");
		}
		ptr = ptr_p;
	}

	template <class T>
	T Read() {
		static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
		if (ptr + sizeof(T) > end) {
			throw SerializationException("Trying to read past end of buffer");
		}
		auto result = Load<T>(ptr);
		ptr += sizeof(T);
		return result;
	}

	template <class T>
	T ReadBigEndian() {
		static_assert(std::is_floating_point<T>::value || std::is_integral<T>::value,
		              "T must be a floating point or integral type");
		if (ptr + sizeof(T) > end) {
			throw SerializationException("Trying to read past end of buffer");
		}

		uint8_t in[sizeof(T)];
		uint8_t out[sizeof(T)];
		memcpy(in, ptr, sizeof(T));
		ptr += sizeof(T);

		for (size_t i = 0; i < sizeof(T); i++) {
			out[i] = in[sizeof(T) - i - 1];
		}
		T swapped = 0;
		memcpy(&swapped, out, sizeof(T));
		return swapped;
	}

	template <class T>
	void Write(T value) {
		static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
		if (ptr + sizeof(T) > end) {
			throw SerializationException("Trying to write past end of buffer");
		}
		Store<T>(value, ptr);
		ptr += sizeof(T);
	}

	template <class T>
	T Peek() {
		static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
		if (ptr + sizeof(T) > end) {
			throw SerializationException("Trying to read past end of buffer");
		}
		return Load<T>(ptr);
	}

	template <class T>
	void Skip() {
		static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
		Skip(sizeof(T));
	}

	void Skip(uint32_t bytes) {
		if (ptr + bytes > end) {
			throw SerializationException("Trying to read past end of buffer");
		}
		ptr += bytes;
	}

	void Seek(Offset offset, int32_t bytes) {
		switch (offset) {
		case Offset::START:
			ptr = start + bytes;
			break;
		case Offset::CURRENT:
			ptr += bytes;
			break;
		case Offset::END:
			ptr = end + bytes;
			break;
		}
		if (ptr < start || ptr > end) {
			throw SerializationException("Trying to set ptr outside of buffer");
		}
	}
};

} // namespace core

} // namespace spatial