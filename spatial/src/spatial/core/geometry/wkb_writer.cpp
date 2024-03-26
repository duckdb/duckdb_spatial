#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Size Calculator
//------------------------------------------------------------------------------
class WKBSizeCalculator final : GeometryProcessor<uint32_t> {
	uint32_t ProcessPoint(const VertexData &vertices) override {
		// <byte order> + <type> + <x> + <y> (+ <z> + <m>)
		// WKB Points always write points even if empty
		return sizeof(uint8_t) + sizeof(uint32_t) + sizeof(double) * (2 + (HasZ() ? 1 : 0) + (HasM() ? 1 : 0));
	}

	uint32_t ProcessLineString(const VertexData &vertices) override {
		// <byte order> + <type> + <count> + <points>
		return sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t) + vertices.ByteSize();
	}

	uint32_t ProcessPolygon(PolygonState &state) override {
		// <byte order> + <type> + <ring_count> + <rings>
		uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
		while (!state.IsDone()) {
			// <count> + <points>
			size += sizeof(uint32_t) + state.Next().ByteSize();
		}
		return size;
	}

	uint32_t ProcessCollection(CollectionState &state) override {
		// <byte order> + <type> + <geometry_count>
		uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
		while (!state.IsDone()) {
			// + <geometry>
			size += state.Next();
		}
		return size;
	}

public:
	uint32_t Execute(const geometry_t &geometry) {
		return Process(geometry);
	}
};

//------------------------------------------------------------------------------
// Serializer
//------------------------------------------------------------------------------
class WKBSerializer final : GeometryProcessor<void, Cursor &> {

	void WriteHeader(Cursor &cursor) {
		// <byte order>
		cursor.Write<uint8_t>(1);
		uint32_t type_id = static_cast<uint32_t>(CurrentType()) + 1;
		if (HasZ()) {
			type_id += 1000;
		}
		if (HasM()) {
			type_id += 2000;
		}
		// <type>
		cursor.Write<uint32_t>(type_id);
	}

	void ProcessPoint(const VertexData &vertices, Cursor &cursor) override {
		WriteHeader(cursor);
		if (vertices.IsEmpty()) {
			cursor.Write(std::numeric_limits<double>::quiet_NaN());
			cursor.Write(std::numeric_limits<double>::quiet_NaN());
			if (HasZ()) {
				cursor.Write(std::numeric_limits<double>::quiet_NaN());
			}
			if (HasM()) {
				cursor.Write(std::numeric_limits<double>::quiet_NaN());
			}
		} else {
			cursor.Write(Load<double>(vertices.data[0]));
			cursor.Write(Load<double>(vertices.data[1]));
			if (HasZ()) {
				cursor.Write(Load<double>(vertices.data[2]));
			}
			if (HasM()) {
				cursor.Write(Load<double>(vertices.data[3]));
			}
		}
	}

	void ProcessVertices(const VertexData &vertices, Cursor &cursor) {
		bool has_z = HasZ();
		bool has_m = HasM();
		for (uint32_t i = 0; i < vertices.count; i++) {
			cursor.Write(Load<double>(vertices.data[0] + i * vertices.stride[0]));
			cursor.Write(Load<double>(vertices.data[1] + i * vertices.stride[1]));
			if (has_z) {
				cursor.Write(Load<double>(vertices.data[2] + i * vertices.stride[2]));
			}
			if (has_m) {
				cursor.Write(Load<double>(vertices.data[3] + i * vertices.stride[3]));
			}
		}
	}

	void ProcessLineString(const VertexData &vertices, Cursor &cursor) override {
		WriteHeader(cursor);
		cursor.Write<uint32_t>(vertices.count);
		ProcessVertices(vertices, cursor);
	}

	void ProcessPolygon(PolygonState &state, Cursor &cursor) override {
		WriteHeader(cursor);
		cursor.Write<uint32_t>(state.RingCount());
		while (!state.IsDone()) {
			auto vertices = state.Next();
			cursor.Write<uint32_t>(vertices.count);
			ProcessVertices(vertices, cursor);
		}
	}

	void ProcessCollection(CollectionState &state, Cursor &cursor) override {
		WriteHeader(cursor);
		cursor.Write<uint32_t>(state.ItemCount());
		while (!state.IsDone()) {
			state.Next(cursor);
		}
	}

public:
	void Execute(const geometry_t &geometry, data_ptr_t start, data_ptr_t end) {
		Cursor cursor(start, end);
		Process(geometry, cursor);
	}
	void Execute(const geometry_t &geometry, string_t &blob) {
		Cursor cursor(blob);
		Process(geometry, cursor);
		blob.Finalize();
	}
};

string_t WKBWriter::Write(const geometry_t &geometry, Vector &result) {
	WKBSizeCalculator size_processor;
	WKBSerializer serializer;
	auto size = size_processor.Execute(geometry);
	auto blob = StringVector::EmptyString(result, size);
	serializer.Execute(geometry, blob);
	return blob;
}

void WKBWriter::Write(const geometry_t &geometry, vector<data_t> &buffer) {
	WKBSizeCalculator size_processor;
	WKBSerializer serializer;
	auto size = size_processor.Execute(geometry);
	buffer.resize(size);
	serializer.Execute(geometry, buffer.data(), buffer.data() + size);
}

const_data_ptr_t WKBWriter::Write(const geometry_t &geometry, uint32_t *size, ArenaAllocator &allocator) {
	WKBSizeCalculator size_processor;
	WKBSerializer serializer;
	auto blob_size = size_processor.Execute(geometry);
	auto blob = allocator.AllocateAligned(blob_size);
	serializer.Execute(geometry, blob, blob + blob_size);
	*size = blob_size;
	return blob;
}

} // namespace core

} // namespace spatial
