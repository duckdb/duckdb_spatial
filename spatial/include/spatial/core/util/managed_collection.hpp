#pragma once

namespace spatial {

namespace core {

struct ManagedCollectionAppendState;
struct ManagedCollectionScanState;

class ManagedCollectionBlock {
public:
	shared_ptr<BlockHandle> handle;
	idx_t item_capacity;
	idx_t item_count;

	explicit ManagedCollectionBlock(idx_t item_capacity) : handle(nullptr), item_capacity(item_capacity), item_count(0) {};
	ManagedCollectionBlock(shared_ptr<BlockHandle> handle, idx_t item_capacity)
		: handle(std::move(handle)), item_capacity(item_capacity), item_count(0) {}
};

template<class T>
class ManagedCollection {
	static constexpr idx_t BLOCK_CAPACITY = Storage::BLOCK_SIZE / sizeof(T);
	BufferManager &manager;
	vector<ManagedCollectionBlock> blocks;
	idx_t size = 0;
public:

	explicit ManagedCollection(BufferManager &manager) : manager(manager) {}

	// Initialize append state, optionally with a lower initial capacity. This is useful for small collections.
	void InitializeAppend(ManagedCollectionAppendState &state, idx_t initial_capacity = BLOCK_CAPACITY);

	// Push a single value to the collection, potentially allocating a new block
	void Append(ManagedCollectionAppendState &state, const T &value);

	// Push multiple values to the collection, allocating as many blocks as neccessary.
	// This is more efficient than pushing values one by one as we keep the current block pinned until it is full.
	void Append(ManagedCollectionAppendState &state, const T *begin, const T *end);

	// Initialize scan state
	void InitializeScan(ManagedCollectionScanState &state, bool destroy_scanned);

	// Scan the collection, writing as many elements as possible to the output buffer
	// Returns the pointer one past the last element written to the output buffer
	template<class ITERATOR>
	ITERATOR Scan(ManagedCollectionScanState &state, ITERATOR begin, ITERATOR end);

	T Fetch(idx_t idx);

	idx_t Count() const { return size; }

	void Clear() {
		blocks.clear();
		size = 0;
	}
};

template<class T>
constexpr idx_t ManagedCollection<T>::BLOCK_CAPACITY;

struct ManagedCollectionAppendState {
	BufferHandle handle;
	ManagedCollectionBlock* block = nullptr;
};

template <class T>
void ManagedCollection<T>::InitializeAppend(ManagedCollectionAppendState &state, idx_t initial_capacity) {
	D_ASSERT(initial_capacity <= BLOCK_CAPACITY);

	//state.block.item_count = 0;
	//state.block.item_capacity = initial_capacity;

	if(initial_capacity < BLOCK_CAPACITY) {
		// Allocate a new small block
		blocks.emplace_back(manager.RegisterSmallMemory(initial_capacity * sizeof(T)), initial_capacity);
		state.block = &blocks.back();
		state.block->item_count = 0;
		state.block->item_capacity = initial_capacity;
		state.handle = manager.Pin(state.block->handle);
	} else {
		// Allocate a new standard block
		blocks.emplace_back(initial_capacity);
		state.block = &blocks.back();
		state.block->item_count = 0;
		state.block->item_capacity = initial_capacity;
		state.handle = manager.Allocate(MemoryTag::EXTENSION, Storage::BLOCK_SIZE, true, &state.block->handle);
	}
}

template<class T>
void ManagedCollection<T>::Append(ManagedCollectionAppendState &state, const T *begin, const T *end) {
	while(begin != end) {
		if(state.block->item_count >= state.block->item_capacity) {
			// Allocate a new standard block
			blocks.emplace_back(BLOCK_CAPACITY);
			state.block = &blocks.back();
			state.handle = manager.Allocate(MemoryTag::EXTENSION, Storage::BLOCK_SIZE, true, &state.block->handle);
			state.block->item_count = 0;
			state.block->item_capacity = BLOCK_CAPACITY;
		}

		// Compute how much we can copy before the block is full or we run out of elements
		auto remaining_capacity = state.block->item_capacity - state.block->item_count;
		auto remaining_elements = end - begin;
		auto to_copy = MinValue<idx_t>(remaining_capacity, remaining_elements);

		// Get the writeable pointer to the block
		auto ptr = state.handle.Ptr() + state.block->item_count * sizeof(T);

		// Store as many elements as we can
		for(idx_t i = 0; i < to_copy; i++) {
			Store<T>(*begin, ptr);
			++begin;
			++size;
			++state.block->item_count;
			ptr += sizeof(T);
		}
	}
}

template<class T>
void ManagedCollection<T>::Append(ManagedCollectionAppendState &state, const T &value) {
	Append(state, &value, &value + 1);
}

struct ManagedCollectionScanState {
	// The index of the current block
	idx_t block_idx = 0;
	idx_t total_blocks = 0;
	bool destroy_scanned = false;

	idx_t scan_idx = 0;
	idx_t scan_capacity = 0;
	BufferHandle handle;

	bool IsDone() const {
		return block_idx >= total_blocks;
	}
};

template <class T>
void ManagedCollection<T>::InitializeScan(ManagedCollectionScanState &state, bool destroy_scanned) {
	state.block_idx = 0;
	state.total_blocks = blocks.size();
	state.destroy_scanned = destroy_scanned;

	auto &block = blocks[state.block_idx];
	state.handle = manager.Pin(block.handle);
	state.scan_capacity = block.item_count; // We can only scan the items that were actually written
	state.scan_idx = 0;
}

template <class T>
template <class ITERATOR>
ITERATOR ManagedCollection<T>::Scan(ManagedCollectionScanState &state, ITERATOR begin, ITERATOR end) {
	while (begin != end) {
		if(state.scan_idx >= state.scan_capacity) {

			if(state.destroy_scanned) {
				// Destroy the current block
				state.handle.Destroy();
			}

			// Move to the next block
			state.block_idx++;
			if(state.block_idx >= blocks.size()) {
				// We reached the end of the collection
				break;
			}

			auto &block = blocks[state.block_idx];
			state.handle = manager.Pin(block.handle);
			state.scan_capacity = block.item_count; // We can only scan the items that were actually written
			state.scan_idx = 0;
		}

		// Compute how much we can copy before the block is empty or we run out of space
		const auto remaining_capacity = state.scan_capacity - state.scan_idx;
		const auto remaining_elements = end - begin;
		const auto to_copy = MinValue<idx_t>(remaining_capacity, remaining_elements);

		// Get the readable pointer to the block
		auto ptr = state.handle.Ptr() + state.scan_idx * sizeof(T);

		// Copy as many elements as we can
		for(idx_t i = 0; i < to_copy; i++) {
			*begin = Load<T>(ptr);
			++begin;
			++state.scan_idx;
			ptr += sizeof(T);
		}
	}
	return begin;
}

template<class T>
T ManagedCollection<T>::Fetch(idx_t idx) {
	auto block_idx = idx / BLOCK_CAPACITY;
	auto block_offset = idx % BLOCK_CAPACITY;
	auto &block = blocks[block_idx];
	auto ptr = manager.Pin(block.handle).Ptr() + block_offset * sizeof(T);
	return Load<T>(ptr);
}

} // namespace core

} // namespace spatial