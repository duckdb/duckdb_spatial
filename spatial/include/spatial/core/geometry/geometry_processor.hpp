#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/util/cursor.hpp"
#include "spatial/core/geometry/geometry_type.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------
// GeometryProcessor
//------------------------------------------------------------------------
// The GeometryProcessor class is used to process a serialized geometry.
// By subclassing and overriding the appropriate methods an algorithm
// can be implemented to process the geometry in a streaming fashion.
//------------------------------------------------------------------------

//------------------------------------------------------------------------
// VertexData
//------------------------------------------------------------------------
class VertexData {
private:
	static const constexpr double EMPTY_DATA = 0;

public:
	// The M axis is always in the fourth position and the Z axis is always in the third position
	const_data_ptr_t data[4] = {const_data_ptr_cast(&EMPTY_DATA), const_data_ptr_cast(&EMPTY_DATA),
	                            const_data_ptr_cast(&EMPTY_DATA), const_data_ptr_cast(&EMPTY_DATA)};
	ptrdiff_t stride[4] = {0, 0, 0, 0};
	uint32_t count = 0;

	VertexData(const_data_ptr_t data_ptr, uint32_t count, bool has_z, bool has_m) : count(count) {
		// Get the data at the current cursor position

		// TODO: These calculations are all constant, we could move it to Execute() instead.
		// TODO: Add GetX, GetY, GetZ, GetM methods
		data[0] = data_ptr;
		data[1] = data_ptr + sizeof(double);
		if (has_z && has_m) {
			data[2] = data_ptr + 2 * sizeof(double);
			data[3] = data_ptr + 3 * sizeof(double);
		} else if (has_z) {
			data[2] = data_ptr + 2 * sizeof(double);
		} else if (has_m) {
			data[3] = data_ptr + 2 * sizeof(double);
		}

		auto vertex_size = static_cast<ptrdiff_t>(sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0)));

		stride[0] = vertex_size;
		stride[1] = vertex_size;
		stride[2] = has_z ? vertex_size : 0;
		stride[3] = has_m ? vertex_size : 0;
	}

	bool IsEmpty() const {
		return count == 0;
	}

	uint32_t ByteSize() const {
		return count * sizeof(double) * (2 + (stride[2] != 0) + (stride[3] != 0));
	}
};

//------------------------------------------------------------------------
// Helper so that we can return void from functions generically
//------------------------------------------------------------------------
template <class RESULT>
class ResultWrapper {
private:
	RESULT tmp;

public:
	template <class F>
	explicit ResultWrapper(F &&f) : tmp(std::move(f())) {
	}
	ResultWrapper(const ResultWrapper &other) = delete;
	ResultWrapper &operator=(const ResultWrapper &other) = delete;
	ResultWrapper(ResultWrapper &&other) = delete;
	ResultWrapper &operator=(ResultWrapper &&other) = delete;
	RESULT &&ReturnAndDestroy() {
		return std::move(tmp);
	}
};

template <>
class ResultWrapper<void> {
public:
	template <class F>
	explicit ResultWrapper(F &&f) {
		f();
	}
	ResultWrapper(const ResultWrapper &other) = delete;
	ResultWrapper &operator=(const ResultWrapper &other) = delete;
	ResultWrapper(ResultWrapper &&other) = delete;
	ResultWrapper &operator=(ResultWrapper &&other) = delete;
	void ReturnAndDestroy() {
	}
};

//------------------------------------------------------------------------
// GeometryProcessor
//------------------------------------------------------------------------
template <class RESULT = void, class... ARGS>
class GeometryProcessor {
private:
	bool has_z = false;
	bool has_m = false;
	uint32_t nesting_level = 0;
	GeometryType current_type = GeometryType::POINT;
	GeometryType parent_type = GeometryType::POINT;

protected:
	bool HasZ() const {
		return has_z;
	}
	bool HasM() const {
		return has_m;
	}
	bool IsNested() const {
		return nesting_level > 0;
	}
	uint32_t NestingLevel() const {
		return nesting_level;
	}
	GeometryType CurrentType() const {
		return current_type;
	}
	GeometryType ParentType() const {
		return parent_type;
	}

	class CollectionState {
	private:
		friend class GeometryProcessor<RESULT, ARGS...>;
		uint32_t item_count;
		uint32_t current_item;
		GeometryProcessor<RESULT, ARGS...> &processor;
		Cursor &cursor;
		CollectionState(uint32_t item_count, GeometryProcessor<RESULT, ARGS...> &processor, Cursor &cursor)
		    : item_count(item_count), current_item(0), processor(processor), cursor(cursor) {
		}

	public:
		CollectionState(const CollectionState &other) = delete;
		CollectionState &operator=(const CollectionState &other) = delete;
		CollectionState(CollectionState &&other) = delete;
		CollectionState &operator=(CollectionState &&other) = delete;

		uint32_t ItemCount() const {
			return item_count;
		}
		bool IsDone() const {
			return current_item >= item_count;
		}

		// NOLINTNEXTLINE
		RESULT Next(ARGS... args) {
			// Save parent type and increment nesting
			auto prev_parent_type = processor.parent_type;
			processor.parent_type = processor.current_type;
			processor.nesting_level++;
			// NOLINTNEXTLINE
			ResultWrapper<RESULT> result([&]() { return processor.ReadGeometry(cursor, args...); });

			// Restore parent type and decrement nesting
			processor.current_type = processor.parent_type;
			processor.parent_type = prev_parent_type;
			processor.nesting_level--;

			// Also move state forwards
			current_item++;

			return result.ReturnAndDestroy();
		}
	};

	class PolygonState {
	private:
		friend class GeometryProcessor<RESULT, ARGS...>;
		uint32_t ring_count;
		uint32_t current_ring;
		const_data_ptr_t count_ptr;
		const_data_ptr_t data_ptr;
		GeometryProcessor<RESULT, ARGS...> &processor;
		explicit PolygonState(uint32_t ring_count, const_data_ptr_t count_ptr, const_data_ptr_t data_ptr,
		                      GeometryProcessor<RESULT, ARGS...> &processor)
		    : ring_count(ring_count), current_ring(0), count_ptr(count_ptr), data_ptr(data_ptr), processor(processor) {
		}

	public:
		PolygonState(const PolygonState &other) = delete;
		PolygonState &operator=(const PolygonState &other) = delete;
		PolygonState(PolygonState &&other) = delete;
		PolygonState &operator=(PolygonState &&other) = delete;

		uint32_t RingCount() const {
			return ring_count;
		}
		bool IsDone() const {
			return current_ring == ring_count;
		}
		VertexData Next() {
			auto count = Load<uint32_t>(count_ptr);
			VertexData data(data_ptr, count, processor.HasZ(), processor.HasM());
			current_ring++;
			count_ptr += sizeof(uint32_t);
			data_ptr += count * sizeof(double) * (2 + (processor.HasZ() ? 1 : 0) + (processor.HasM() ? 1 : 0));
			return data;
		}
	};

	virtual RESULT ProcessPoint(const VertexData &vertices, ARGS... args) = 0;
	virtual RESULT ProcessLineString(const VertexData &vertices, ARGS... args) = 0;
	virtual RESULT ProcessPolygon(PolygonState &state, ARGS... args) = 0;
	virtual RESULT ProcessCollection(CollectionState &state, ARGS... args) = 0;

public:
	RESULT Process(const geometry_t &geom, ARGS... args) {

		has_z = geom.GetProperties().HasZ();
		has_m = geom.GetProperties().HasM();
		nesting_level = 0;
		current_type = geom.GetType();
		parent_type = GeometryType::POINT;

		Cursor cursor(geom);

		cursor.Skip<GeometryType>();
		cursor.Skip<GeometryProperties>();
		cursor.Skip<uint16_t>();
		cursor.Skip<uint32_t>();

		auto dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);
		auto has_bbox = geom.GetProperties().HasBBox();
		auto bbox_size = has_bbox ? dims * 2 * sizeof(float) : 0;
		cursor.Skip(bbox_size);

		return ReadGeometry(cursor, args...);
	}

private:
	RESULT ReadGeometry(Cursor &cursor, ARGS... args) {
		auto type = cursor.Peek<SerializedGeometryType>();
		switch (type) {
		case SerializedGeometryType::POINT:
			current_type = GeometryType::POINT;
			return ReadPoint(cursor, args...);
		case SerializedGeometryType::LINESTRING:
			current_type = GeometryType::LINESTRING;
			return ReadLineString(cursor, args...);
		case SerializedGeometryType::POLYGON:
			current_type = GeometryType::POLYGON;
			return ReadPolygon(cursor, args...);
		case SerializedGeometryType::MULTIPOINT:
			current_type = GeometryType::MULTIPOINT;
			return ReadCollection(cursor, args...);
		case SerializedGeometryType::MULTILINESTRING:
			current_type = GeometryType::MULTILINESTRING;
			return ReadCollection(cursor, args...);
		case SerializedGeometryType::MULTIPOLYGON:
			current_type = GeometryType::MULTIPOLYGON;
			return ReadCollection(cursor, args...);
		case SerializedGeometryType::GEOMETRYCOLLECTION:
			current_type = GeometryType::GEOMETRYCOLLECTION;
			return ReadCollection(cursor, args...);
		default:
			throw SerializationException("Unknown geometry type (%ud)", static_cast<uint32_t>(type));
		}
	}

	RESULT ReadPoint(Cursor &cursor, ARGS... args) {
		auto type = cursor.Read<SerializedGeometryType>();
		D_ASSERT(type == SerializedGeometryType::POINT);
		(void)type;
		auto count = cursor.Read<uint32_t>();
		VertexData data(cursor.GetPtr(), count, HasZ(), HasM());
		cursor.Skip(data.ByteSize());
		return ProcessPoint(data, args...);
	}

	RESULT ReadLineString(Cursor &cursor, ARGS... args) {
		auto type = cursor.Read<SerializedGeometryType>();
		D_ASSERT(type == SerializedGeometryType::LINESTRING);
		(void)type;
		auto count = cursor.Read<uint32_t>();
		VertexData data(cursor.GetPtr(), count, HasZ(), HasM());
		cursor.Skip(data.ByteSize());
		return ProcessLineString(data, args...);
	}

	RESULT ReadPolygon(Cursor &cursor, ARGS... args) {
		auto type = cursor.Read<SerializedGeometryType>();
		D_ASSERT(type == SerializedGeometryType::POLYGON);
		(void)type;
		auto ring_count = cursor.Read<uint32_t>();
		auto count_ptr = cursor.GetPtr();
		cursor.Skip(ring_count * sizeof(uint32_t) + ((ring_count % 2) * sizeof(uint32_t)));
		PolygonState state(ring_count, count_ptr, cursor.GetPtr(), *this);

		ResultWrapper<RESULT> result([&]() { return ProcessPolygon(state, args...); });

		if (IsNested()) {
			// Consume the rest of the polygon so we can continue processing the parent
			while (!state.IsDone()) {
				state.Next();
			}
		}
		cursor.SetPtr(const_cast<data_ptr_t>(state.data_ptr));

		return result.ReturnAndDestroy();
	}

	// NOLINTNEXTLINE
	RESULT ReadCollection(Cursor &cursor, ARGS... args) {
		auto type = cursor.Read<SerializedGeometryType>();
		(void)type;
		auto count = cursor.Read<uint32_t>();
		CollectionState state(count, *this, cursor);

		ResultWrapper<RESULT> result([&]() { return ProcessCollection(state, args...); });

		if (IsNested()) {
			// Consume the rest of the collection so we can continue processing the parent
			while (!state.IsDone()) {
				state.Next(args...);
			}
		}

		return result.ReturnAndDestroy();
	}
};

} // namespace core

} // namespace spatial