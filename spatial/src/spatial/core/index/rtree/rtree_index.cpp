#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_scanner.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "spatial/core/geometry/geometry_type.hpp"
#include "spatial/core/index/rtree/rtree_module.hpp"
#include "spatial/core/index/rtree/rtree_node.hpp"
#include "spatial/core/util/math.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// RTree Index Scan State
//------------------------------------------------------------------------------
class RTreeIndexScanState final : public IndexScanState {
public:
	RTreeBounds query_bounds;
	RTreeScanner scanner;
};

//------------------------------------------------------------------------------
// RTreeIndex Methods
//------------------------------------------------------------------------------

// Constructor
RTreeIndex::RTreeIndex(const string &name, IndexConstraintType index_constraint_type,
                       const vector<column_t> &column_ids, TableIOManager &table_io_manager,
                       const vector<unique_ptr<Expression>> &unbound_expressions, AttachedDatabase &db,
                       const case_insensitive_map_t<Value> &options, const IndexStorageInfo &info,
                       idx_t estimated_cardinality)
    : BoundIndex(name, TYPE_NAME, index_constraint_type, column_ids, table_io_manager, unbound_expressions, db) {

	if (index_constraint_type != IndexConstraintType::NONE) {
		throw NotImplementedException("RTree indexes do not support unique or primary key constraints");
	}

	// Defaults
	idx_t max_node_capacity = 64;
	idx_t min_node_capacity = 24;

	const auto max_cap_param_search = options.find("max_node_capacity");
	if (max_cap_param_search != options.end()) {
		const auto val = max_cap_param_search->second.GetValue<int32_t>();
		if (val < 4) {
			throw InvalidInputException("RTree: max_node_capacity must be at least 4");
		}
		if (val > 255) {
			throw InvalidInputException("RTree: max_node_capacity must be at most 255");
		}
		max_node_capacity = UnsafeNumericCast<idx_t>(val);
	}

	const auto min_cap_search = options.find("min_node_capacity");
	if (min_cap_search != options.end()) {
		const auto val = min_cap_search->second.GetValue<int32_t>();
		if (val < 0) {
			throw InvalidInputException("RTree: min_node_capacity must be at least 0");
		}
		if (val > max_node_capacity / 2) {
			throw InvalidInputException("RTree: min_node_capacity must be at most 'max_node_capacity / 2'");
		}
		min_node_capacity = UnsafeNumericCast<idx_t>(val);
	} else {
		// If no min capacity is set, set it to 40% of the max capacity
		if (max_cap_param_search != options.end()) {
			min_node_capacity = std::ceil(static_cast<double>(max_node_capacity) * 0.4);
		}
	}

	// TODO: Check the block manager for the current block size and see if our bounds are ok

	// Create the RTree
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	tree = make_uniq<RTree>(block_manager, max_node_capacity, min_node_capacity);

	if (info.IsValid()) {
		// This is an old index that needs to be loaded
		// Initialize the allocators
		tree->GetAllocator().Init(info.allocator_infos[0]);
		// Set the root node and recalculate the bounds
		tree->SetRoot(info.root);
	}
}

unique_ptr<IndexScanState> RTreeIndex::InitializeScan(const RTreeBounds &query) const {
	auto state = make_uniq<RTreeIndexScanState>();
	state->query_bounds = query;
	auto &root = tree->GetRoot();
	if (root.IsSet() && state->query_bounds.Intersects(root.bounds)) {
		state->scanner.Init(root);
	}
	return std::move(state);
}

idx_t RTreeIndex::Scan(IndexScanState &state, Vector &result) const {
	auto &sstate = state.Cast<RTreeIndexScanState>();
	const auto row_ids = FlatVector::GetData<row_t>(result);

	idx_t output_idx = 0;
	sstate.scanner.Scan(*tree, [&](const RTreeEntry &entry, const idx_t &) {
		// Does this entry intersect with the query bounds?
		if (!sstate.query_bounds.Intersects(entry.bounds)) {
			// No, skip it
			return RTreeScanResult::SKIP;
		}
		// Is this a row id?
		if (entry.pointer.IsRowId()) {
			row_ids[output_idx++] = entry.pointer.GetRowId();
			// Have we filled the result vector?
			if (output_idx == STANDARD_VECTOR_SIZE) {
				return RTreeScanResult::YIELD;
			}
		}
		// Continue scanning
		return RTreeScanResult::CONTINUE;
	});
	return output_idx;
}

void RTreeIndex::CommitDrop(IndexLock &index_lock) {
	// TODO: Maybe we can drop these much earlier?
	tree->Reset();
}

ErrorData RTreeIndex::Insert(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	// TODO: Dont flatten chunk
	input.Flatten();

	const auto &geom_vec = FlatVector::GetData<geometry_t>(input.data[0]);
	const auto &rowid_data = FlatVector::GetData<row_t>(rowid_vec);

	RTreeEntry entry_buffer[STANDARD_VECTOR_SIZE];

	for (idx_t i = 0; i < input.size(); i++) {
		const auto rowid = rowid_data[i];

		Box2D<double> box_2d;
		if (!geom_vec[i].TryGetCachedBounds(box_2d)) {
			continue;
		}

		Box2D<float> bbox;
		bbox.min.x = MathUtil::DoubleToFloatDown(box_2d.min.x);
		bbox.min.y = MathUtil::DoubleToFloatDown(box_2d.min.y);
		bbox.max.x = MathUtil::DoubleToFloatUp(box_2d.max.x);
		bbox.max.y = MathUtil::DoubleToFloatUp(box_2d.max.y);

		entry_buffer[i] = {RTree::MakeRowId(rowid), bbox};
	}

	// TODO: Investigate this more, is there a better way to insert multiple entries
	// so that they produce a better tree?
	// E.g. sort by x coordinate, or hilbert sort? or STR packing?
	// Or insert by smallest first? or largest first?
	// Or even create a separate subtree entirely, and then insert that into the root?
	for (idx_t i = 0; i < input.size(); i++) {
		tree->Insert(entry_buffer[i]);
	}

	return ErrorData {};
}

ErrorData RTreeIndex::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	return Insert(lock, appended_data, row_identifiers);
}

void RTreeIndex::VerifyAppend(DataChunk &chunk) {
	// There is nothing to verify here as we dont support constraints anyway
}

void RTreeIndex::VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) {
	// There is nothing to verify here as we dont support constraints anyway
}

void RTreeIndex::Delete(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	const auto count = input.size();

	UnifiedVectorFormat geom_format;
	UnifiedVectorFormat rowid_format;

	input.data[0].ToUnifiedFormat(count, geom_format);
	rowid_vec.ToUnifiedFormat(count, rowid_format);

	for (idx_t i = 0; i < count; i++) {
		const auto geom_idx = geom_format.sel->get_index(i);
		const auto rowid_idx = rowid_format.sel->get_index(i);

		if (!geom_format.validity.RowIsValid(geom_idx) || !rowid_format.validity.RowIsValid(rowid_idx)) {
			continue;
		}

		auto &geom = UnifiedVectorFormat::GetData<geometry_t>(geom_format)[geom_idx];
		auto &rowid = UnifiedVectorFormat::GetData<row_t>(rowid_format)[rowid_idx];

		Box2D<double> raw_bounds;
		if (!geom.TryGetCachedBounds(raw_bounds)) {
			continue;
		}

		Box2D<float> approx_bounds;
		approx_bounds.min.x = MathUtil::DoubleToFloatDown(raw_bounds.min.x);
		approx_bounds.min.y = MathUtil::DoubleToFloatDown(raw_bounds.min.y);
		approx_bounds.max.x = MathUtil::DoubleToFloatUp(raw_bounds.max.x);
		approx_bounds.max.y = MathUtil::DoubleToFloatUp(raw_bounds.max.y);

		RTreeEntry new_entry = {RTree::MakeRowId(rowid), approx_bounds};
		tree->Delete(new_entry);
	}
}

IndexStorageInfo RTreeIndex::GetStorageInfo(const bool get_buffers) {

	IndexStorageInfo info;
	info.name = name;
	info.root = tree->GetRoot().pointer.Get();

	auto &allocator = tree->GetAllocator();

	if (!get_buffers) {
		// use the partial block manager to serialize all allocator data
		auto &block_manager = table_io_manager.GetIndexBlockManager();
		PartialBlockManager partial_block_manager(block_manager, PartialBlockType::FULL_CHECKPOINT);
		allocator.SerializeBuffers(partial_block_manager);
		partial_block_manager.FlushPartialBlocks();
	} else {
		info.buffers.push_back(allocator.InitSerializationToWAL());
	}

	info.allocator_infos.push_back(allocator.GetInfo());
	return info;
}

idx_t RTreeIndex::GetInMemorySize(IndexLock &state) {
	return tree->GetAllocator().GetInMemorySize();
}

bool RTreeIndex::MergeIndexes(IndexLock &state, BoundIndex &other_index) {
	throw NotImplementedException("RTreeIndex::MergeIndexes() not implemented");
}

void RTreeIndex::Vacuum(IndexLock &state) {
}

void RTreeIndex::CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) {
	throw NotImplementedException("RTreeIndex::CheckConstraintsForChunk() not implemented");
}

string RTreeIndex::VerifyAndToString(IndexLock &state, const bool only_verify) {
	throw NotImplementedException("RTreeIndex::VerifyAndToString() not implemented");
}

//------------------------------------------------------------------------------
// Register Index Type
//------------------------------------------------------------------------------
void RTreeModule::RegisterIndex(DatabaseInstance &db) {

	IndexType index_type;

	index_type.name = RTreeIndex::TYPE_NAME;
	index_type.create_instance = [](CreateIndexInput &input) -> unique_ptr<BoundIndex> {
		auto res = make_uniq<RTreeIndex>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
		                                 input.unbound_expressions, input.db, input.options, input.storage_info);
		return std::move(res);
	};

	// Register the index type
	db.config.GetIndexTypes().RegisterIndexType(index_type);
}

} // namespace core

} // namespace spatial