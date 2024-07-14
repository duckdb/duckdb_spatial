#pragma once

#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/index/index_pointer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"

#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct RTreeBounds;

class RTreeIndex final : public BoundIndex {
public:
	// The type name of the RTreeIndex
	static constexpr const char *TYPE_NAME = "RTREE";

public:
	RTreeIndex(const string &name, IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
	          TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
	          AttachedDatabase &db, const case_insensitive_map_t<Value> &options,
	          const IndexStorageInfo &info = IndexStorageInfo(), idx_t estimated_cardinality = 0);


	//! Block pointer to the root of the index
	IndexPointer root_block_ptr;

	//! The allocator used to persist linked blocks
	unique_ptr<FixedSizeAllocator> node_allocator;

	unique_ptr<IndexScanState> InitializeScan(const RTreeBounds &query) const;
	idx_t Scan(IndexScanState &state, Vector &result) const;

	void Construct(DataChunk &input, Vector &row_ids, idx_t thread_idx);
	void PersistToDisk();

public:
	//! Called when data is appended to the index. The lock obtained from InitializeLock must be held
	ErrorData Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Verify that data can be appended to the index without a constraint violation
	void VerifyAppend(DataChunk &chunk) override;
	//! Verify that data can be appended to the index without a constraint violation using the conflict manager
	void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) override;
	//! Deletes all data from the index. The lock obtained from InitializeLock must be held
	void CommitDrop(IndexLock &index_lock) override;
	//! Delete a chunk of entries from the index. The lock obtained from InitializeLock must be held
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Insert a chunk of entries into the index
	ErrorData Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

	IndexStorageInfo GetStorageInfo(const bool get_buffers) override;
	idx_t GetInMemorySize(IndexLock &state) override;

	//! Merge another index into this index. The lock obtained from InitializeLock must be held, and the other
	//! index must also be locked during the merge
	bool MergeIndexes(IndexLock &state, BoundIndex &other_index) override;

	//! Traverses an RTreeIndex and vacuums the qualifying nodes. The lock obtained from InitializeLock must be held
	void Vacuum(IndexLock &state) override;

	//! Performs constraint checking for a chunk of input data
	void CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) override;

	//! Returns the string representation of the RTreeIndex, or only traverses and verifies the index
	string VerifyAndToString(IndexLock &state, const bool only_verify) override;

	string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
	                                     DataChunk &input) override {
		return "Constraint violation in RTree index";
	}
};


/*
class RTreePointer : public IndexPointer {
	bool IsLeaf() const {
		return !HasMetadata();
	}
	bool IsNode() const {
		return HasMetadata();
	}
	explicit RTreePointer(row_t row_id) {
		Set(row_id);
	}
	// todo create from buffer;
};

class RTreeEntry {
	RTreePointer ptr;
	float bbox[4];
};

// The tape is basically a very simple ColumnDataCollection
class ExternalTape {
	static constexpr const idx_t ENTRIES_PER_BLOCK = Storage::BLOCK_SIZE / sizeof(RTreeEntry);
public:
	BufferManager &manager;
	vector<shared_ptr<BlockHandle>> blocks;
	shared_ptr<BlockHandle> current_block;

	// The current amount of entries in the current block;
	idx_t current_entry_count;

public:
	void Reserve(idx_t entry_count) {
		auto required_size = entry_count * sizeof(RTreeEntry);
		if(required_size < Storage::BLOCK_SIZE) {
			// Create a temporary block
			current_block = manager.RegisterSmallMemory(required_size);

			auto buf = manager.Pin(current_block);
			buf.Ptr();
		}
	}

	void Append(DataChunk &chunk) {
		auto count = chunk.size();

		// TODO: Dont flatten
		chunk.Flatten();

		idx_t remaining = count;
		while(remaining > 0) {
			if(current_entry_count == ENTRIES_PER_BLOCK) {
				// Pop a new block
			}

			remaining--;
		}
	}

	void foo() {
		auto size = Storage::BLOCK_SIZE;
		blocks.emplace_back();
		manager.Allocate(MemoryTag::EXTENSION, Storage::BLOCK_SIZE, true, &blocks.back());
	}
};
*/

} // namespace core

} // namespace spatial