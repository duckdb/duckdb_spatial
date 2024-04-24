#include "duckdb/common/file_system.hpp"

#include "spatial/common.hpp"
#include "spatial/core/io/shapefile.hpp"

void SASetupDefaultHooks(SAHooks *hooks) {
	// Should never be called, use OpenLL and pass in the hooks
	throw duckdb::InternalException("SASetupDefaultHooks");
}

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Shapefile filesystem abstractions
//------------------------------------------------------------------------------
static SAFile DuckDBShapefileOpen(void *userData, const char *filename, const char *access_mode) {
	try {
		auto &fs = *reinterpret_cast<FileSystem *>(userData);
		auto file_handle = fs.OpenFile(filename, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (!file_handle) {
			return nullptr;
		}
		return reinterpret_cast<SAFile>(file_handle.release());
	} catch (...) {
		return nullptr;
	}
}

static SAOffset DuckDBShapefileRead(void *p, SAOffset size, SAOffset nmemb, SAFile file) {
	auto handle = reinterpret_cast<FileHandle *>(file);
	auto read_bytes = handle->Read(p, size * nmemb);
	return read_bytes / size;
}

static SAOffset DuckDBShapefileWrite(const void *p, SAOffset size, SAOffset nmemb, SAFile file) {
	auto handle = reinterpret_cast<FileHandle *>(file);
	auto written_bytes = handle->Write(const_cast<void *>(p), size * nmemb);
	return written_bytes / size;
}

static SAOffset DuckDBShapefileSeek(SAFile file, SAOffset offset, int whence) {
	auto file_handle = reinterpret_cast<FileHandle *>(file);
	switch (whence) {
	case SEEK_SET:
		file_handle->Seek(offset);
		break;
	case SEEK_CUR:
		file_handle->Seek(file_handle->SeekPosition() + offset);
		break;
	case SEEK_END:
		file_handle->Seek(file_handle->GetFileSize() + offset);
		break;
	default:
		throw InternalException("Unknown seek type");
	}
	return 0;
}

static SAOffset DuckDBShapefileTell(SAFile file) {
	auto handle = reinterpret_cast<FileHandle *>(file);
	return handle->SeekPosition();
}

static int DuckDBShapefileFlush(SAFile file) {
	try {
		auto handle = reinterpret_cast<FileHandle *>(file);
		handle->Sync();
		return 0;
	} catch (...) {
		return -1;
	}
}

static int DuckDBShapefileClose(SAFile file) {
	try {
		auto handle = reinterpret_cast<FileHandle *>(file);
		handle->Close();
		delete handle;
		return 0;
	} catch (...) {
		return -1;
	}
}

static int DuckDBShapefileRemove(void *userData, const char *filename) {
	try {
		auto &fs = *reinterpret_cast<FileSystem *>(userData);
		auto file = fs.OpenFile(filename, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (!file) {
			return -1;
		}
		auto file_type = fs.GetFileType(*file);
		if (file_type == FileType::FILE_TYPE_DIR) {
			fs.RemoveDirectory(filename);
		} else {
			fs.RemoveFile(filename);
		}
		return 0;
	} catch (...) {
		return -1;
	}
}

static void DuckDBShapefileError(const char *message) {
	// TODO
	// We cant throw an exception here because the shapefile library is not
	// exception safe. Instead we should store it somewhere...
	// Maybe another client context cache?

	// Note that we need to copy the message

	fprintf(stderr, "%s\n", message);
}

//------------------------------------------------------------------------------
// RAII Wrappers
//------------------------------------------------------------------------------

static SAHooks GetDuckDBHooks(FileSystem &fs) {
	SAHooks hooks;
	hooks.FOpen = DuckDBShapefileOpen;
	hooks.FRead = DuckDBShapefileRead;
	hooks.FWrite = DuckDBShapefileWrite;
	hooks.FSeek = DuckDBShapefileSeek;
	hooks.FTell = DuckDBShapefileTell;
	hooks.FFlush = DuckDBShapefileFlush;
	hooks.FClose = DuckDBShapefileClose;
	hooks.Remove = DuckDBShapefileRemove;

	hooks.Error = DuckDBShapefileError;
	hooks.Atof = std::atof;
	hooks.userData = &fs;
	return hooks;
}

DBFHandlePtr OpenDBFFile(FileSystem &fs, const string &filename) {
	auto hooks = GetDuckDBHooks(fs);
	auto handle = DBFOpenLL(filename.c_str(), "rb", &hooks);

	if (!handle) {
		throw IOException("Failed to open DBF file %s", filename.c_str());
	}

	return DBFHandlePtr(handle);
}

SHPHandlePtr OpenSHPFile(FileSystem &fs, const string &filename) {
	auto hooks = GetDuckDBHooks(fs);
	auto handle = SHPOpenLL(filename.c_str(), "rb", &hooks);
	if (!handle) {
		throw IOException("Failed to open SHP file %s", filename);
	}
	return SHPHandlePtr(handle);
}

} // namespace core

} // namespace spatial