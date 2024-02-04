#pragma once
#include "shapefil.h"

namespace spatial {

namespace core {

struct SHPHandleDeleter {
	void operator()(SHPInfo *info) {
		if (info) {
			SHPClose(info);
		}
	}
};
using SHPHandlePtr = unique_ptr<SHPInfo, SHPHandleDeleter>;

struct DBFHandleDeleter {
	void operator()(DBFInfo *info) {
		if (info) {
			DBFClose(info);
		}
	}
};

using DBFHandlePtr = unique_ptr<DBFInfo, DBFHandleDeleter>;

struct SHPObjectDeleter {
	void operator()(SHPObject *obj) {
		if (obj) {
			SHPDestroyObject(obj);
		}
	}
};

using SHPObjectPtr = unique_ptr<SHPObject, SHPObjectDeleter>;

DBFHandlePtr OpenDBFFile(FileSystem &fs, const string &filename);
SHPHandlePtr OpenSHPFile(FileSystem &fs, const string &filename);

enum class AttributeEncoding {
	UTF8,
	LATIN1,
	BLOB,
};

struct EncodingUtil {
	static inline uint8_t GetUTF8ByteLength(data_t first_char) {
		if (first_char < 0x80)
			return 1;
		if (!(first_char & 0x20))
			return 2;
		if (!(first_char & 0x10))
			return 3;
		if (!(first_char & 0x08))
			return 4;
		if (!(first_char & 0x04))
			return 5;
		return 6;
	}
	static inline data_t UTF8ToLatin1Char(const_data_ptr_t ptr) {
		auto len = GetUTF8ByteLength(*ptr);
		if (len == 1) {
			return *ptr;
		}
		uint32_t res = static_cast<data_t>(*ptr & (0xff >> (len + 1))) << ((len - 1) * 6);
		while (--len) {
			res |= (*(++ptr) - 0x80) << ((len - 1) * 6);
		}
		// TODO: Throw exception instead if character can't be encoded?
		return res > 0xff ? '?' : static_cast<data_t>(res);
	}

	// Convert UTF-8 to ISO-8859-1
	// out must be at least the size of in
	static void UTF8ToLatin1Buffer(const_data_ptr_t in, data_ptr_t out) {
		while (*in) {
			*out++ = UTF8ToLatin1Char(in);
		}
		*out = 0;
	}

	// convert ISO-8859-1 to UTF-8
	// mind = blown
	// out must be at least 2x the size of in
	static idx_t LatinToUTF8Buffer(const_data_ptr_t in, data_ptr_t out) {
		idx_t len = 0;
		while (*in) {
			if (*in < 128) {
				*out++ = *in++;
				len += 1;
			} else {
				*out++ = 0xc2 + (*in > 0xbf);
				*out++ = (*in++ & 0x3f) + 0x80;
				len += 2;
			}
		}
		return len;
	}
};

} // namespace core

} // namespace spatial