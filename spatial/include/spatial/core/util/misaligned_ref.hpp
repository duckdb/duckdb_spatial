#pragma once

#include "spatial/common.hpp"

namespace spatial {

namespace core {

template <typename T>
class MisalignedRef {
	using TYPE = typename std::remove_const<T>::type;
	data_ptr_t ptr_;

public:
	MisalignedRef(data_ptr_t ptr) noexcept : ptr_(ptr) {
	}

	operator TYPE() const noexcept {
		return Load<TYPE>(ptr_);
	}
	MisalignedRef &operator=(TYPE const &v) noexcept {
		Store<TYPE>(v, ptr_);
		return *this;
	}
	void reset(data_ptr_t ptr) noexcept {
		ptr_ = ptr;
	}
	data_ptr_t ptr() const noexcept {
		return ptr_;
	}
};

template <typename T>
class ConstMisalignedRef {
	using TYPE = typename std::remove_const<T>::type;
	const_data_ptr_t ptr_;

public:
	ConstMisalignedRef(const_data_ptr_t ptr) noexcept : ptr_(ptr) {
	}
	operator TYPE() const noexcept {
		return Load<TYPE>(ptr_);
	}
	const_data_ptr_t ptr() const noexcept {
		return ptr_;
	}
};

} // namespace core

} // namespace spatial