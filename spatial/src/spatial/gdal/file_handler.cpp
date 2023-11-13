#include "spatial/gdal/file_handler.hpp"

#include "cpl_vsi.h"
#include "cpl_string.h"

namespace spatial {

namespace gdal {

//--------------------------------------------------------------------------
// Local Client Context
//--------------------------------------------------------------------------

static thread_local ClientContext *local_context = nullptr;

void GdalFileHandler::SetLocalClientContext(ClientContext &context) {
	local_context = &context;
}

ClientContext &GdalFileHandler::GetLocalClientContext() {
	if (!local_context) {
		throw InternalException("No local client context set");
	}
	return *local_context;
}

//--------------------------------------------------------------------------
// Required Callbacks
//--------------------------------------------------------------------------

static void *DuckDBOpen(void *, const char *file_name, const char *access) {
	auto &context = GdalFileHandler::GetLocalClientContext();
	auto &fs = context.db->GetFileSystem();

	// TODO: Double check that this is correct
	uint8_t flags;
	auto len = strlen(access);
	if (access[0] == 'r') {
		flags = FileFlags::FILE_FLAGS_READ;
		if (len > 1 && access[1] == '+') {
			flags |= FileFlags::FILE_FLAGS_WRITE;
		}
		if (len > 2 && access[2] == '+') {
			// might be "rb+"
			flags |= FileFlags::FILE_FLAGS_WRITE;
		}
	} else if (access[0] == 'w') {
		flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW;
		if (len > 1 && access[1] == '+') {
			flags |= FileFlags::FILE_FLAGS_READ;
		}
		if (len > 2 && access[2] == '+') {
			// might be "wb+"
			flags |= FileFlags::FILE_FLAGS_READ;
		}
	} else if (access[0] == 'a') {
		flags = FileFlags::FILE_FLAGS_APPEND;
		if (len > 1 && access[1] == '+') {
			flags |= FileFlags::FILE_FLAGS_READ;
		}
		if (len > 2 && access[2] == '+') {
			// might be "ab+"
			flags |= FileFlags::FILE_FLAGS_READ;
		}
	} else {
		throw InternalException("Unknown file access type");
	}

	try {
		string path(file_name);
		auto file = fs.OpenFile(file_name, flags);
		return file.release();
	} catch (std::exception &ex) {
		return nullptr;
	}
}

static vsi_l_offset DuckDBTell(void *file) {
	auto file_handle = static_cast<FileHandle *>(file);
	auto offset = file_handle->SeekPosition();
	return static_cast<vsi_l_offset>(offset);
}

static int DuckDBSeek(void *file, vsi_l_offset offset, int whence) {
	auto file_handle = static_cast<FileHandle *>(file);
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

static size_t DuckDBRead(void *pFile, void *pBuffer, size_t n_size, size_t n_count) {
	auto file_handle = static_cast<FileHandle *>(pFile);
	auto read_bytes = file_handle->Read(pBuffer, n_size * n_count);
	// Return the number of items read
	return static_cast<size_t>(read_bytes / n_size);
}

static size_t DuckDBWrite(void *file, const void *buffer, size_t n_size, size_t n_count) {
	auto file_handle = static_cast<FileHandle *>(file);
	auto written_bytes = file_handle->Write(const_cast<void *>(buffer), n_size * n_count);
	// Return the number of items written
	return static_cast<size_t>(written_bytes / n_size);
}

static int DuckDBEoF(void *file) {
	// TODO: Is this correct?
	auto file_handle = static_cast<FileHandle *>(file);
	return file_handle->SeekPosition() == file_handle->GetFileSize() ? TRUE : FALSE;
}

static int DuckDBTruncate(void *file, vsi_l_offset size) {
	auto file_handle = static_cast<FileHandle *>(file);
	file_handle->Truncate(static_cast<int64_t>(size));
	return 0;
}

static int DuckDBClose(void *file) {
	auto file_handle = static_cast<FileHandle *>(file);
	file_handle->Close();
	delete file_handle;
	return 0;
}

static int DuckDBFlush(void *file) {
	auto file_handle = static_cast<FileHandle *>(file);
	file_handle->Sync();
	return 0;
}

static int DuckDBMakeDir(void *, const char *dir_name, long mode) {
	auto &context = GdalFileHandler::GetLocalClientContext();
	auto &fs = context.db->GetFileSystem();

	fs.CreateDirectory(dir_name);
	return 0;
}

static int DuckDBDeleteDir(void *, const char *dir_name) {
	auto &context = GdalFileHandler::GetLocalClientContext();
	auto &fs = context.db->GetFileSystem();

	fs.RemoveDirectory(dir_name);
	return 0;
}

static char **DuckDBReadDir(void *, const char *dir_name, int max_files) {
	auto &context = GdalFileHandler::GetLocalClientContext();
	auto &fs = context.db->GetFileSystem();

	CPLStringList files;
	auto files_count = 0;
	fs.ListFiles(dir_name, [&](const string &file_name, bool is_dir) {
		if (files_count >= max_files) {
			return;
		}
		files.AddString(file_name.c_str());
		files_count++;
	});
	return files.StealList();
}

static char **DuckDBSiblingFiles(void *, const char *dir_name) {
	auto &context = GdalFileHandler::GetLocalClientContext();
	auto &fs = context.db->GetFileSystem();

	CPLStringList files;
	auto file_vector = fs.Glob(dir_name);
	for (auto &file : file_vector) {
		files.AddString(file.c_str());
	}
	return files.StealList();
}

static int DuckDBStatCallback(void *userData, const char *filename, VSIStatBufL *pstatbuf, int nflags) {
	auto &context = GdalFileHandler::GetLocalClientContext();
	auto &fs = context.db->GetFileSystem();

	if (!fs.FileExists(filename)) {
		return -1;
	}

	auto file = fs.OpenFile(filename, FileFlags::FILE_FLAGS_READ);
	if (!file) {
		return -1;
	}

	pstatbuf->st_size = static_cast<off_t>(file->GetFileSize());

	auto type = file->GetType();
	switch (type) {
	// Thesea re the only three types present on all platforms
	case FileType::FILE_TYPE_REGULAR:
		pstatbuf->st_mode = S_IFREG;
		break;
	case FileType::FILE_TYPE_DIR:
		pstatbuf->st_mode = S_IFDIR;
		break;
	case FileType::FILE_TYPE_CHARDEV:
		pstatbuf->st_mode = S_IFCHR;
		break;
	default:
		throw IOException("Unknown file type");
	}

	/* DuckDB doesnt have anything equivalent to these yet... Hopefully thats ok?
	pstatbuf->st_mtime = file->GetLastModifiedTime();
	pstatbuf->st_uid = file->GetFileOwner();
	pstatbuf->st_gid = file->GetFileGroup();
	pstatbuf->st_ino = file->GetFileInode();
	pstatbuf->st_dev = file->GetFileDevice();
	pstatbuf->st_nlink = file->GetFileLinkCount();
	pstatbuf->st_ctime = file->GetFileCreationTime();
	pstatbuf->st_atime = file->GetFileAccessTime();
	 */

	return 0;
}
//--------------------------------------------------------------------------
// Register
//--------------------------------------------------------------------------
void GdalFileHandler::Register() {

	auto callbacks = VSIAllocFilesystemPluginCallbacksStruct();

	callbacks->nCacheSize = 16384000; // same as /vsicurl/
	callbacks->open = DuckDBOpen;
	callbacks->read = DuckDBRead;
	callbacks->write = DuckDBWrite;
	callbacks->close = DuckDBClose;
	callbacks->tell = DuckDBTell;
	callbacks->seek = DuckDBSeek;
	callbacks->eof = DuckDBEoF;
	callbacks->flush = DuckDBFlush;
	callbacks->truncate = DuckDBTruncate;
	callbacks->mkdir = DuckDBMakeDir;
	callbacks->rmdir = DuckDBDeleteDir;
	callbacks->read_dir = DuckDBReadDir;
	callbacks->sibling_files = DuckDBSiblingFiles;
	callbacks->stat = DuckDBStatCallback;

	// Override this as the default file system
	VSIInstallPluginHandler("", callbacks);
}

} // namespace gdal

} // namespace spatial
