#pragma once

#include "spatial/common.hpp"
#include "spatial/core/util/misaligned_ref.hpp"

namespace spatial {

namespace core {

template <class TYPE, class PTR, class pointer, class reference>
class MisalignedPtrBase {
private:
	PTR ptr_;

public:
	using iterator_category = std::random_access_iterator_tag;
	using value_type = TYPE;
	using difference_type = std::ptrdiff_t;

	reference operator*() const noexcept {
		return {ptr_};
	}
	reference operator[](std::size_t i) noexcept {
		return reference(ptr_ + i * sizeof(TYPE));
	}
	value_type operator[](std::size_t i) const noexcept {
		return Load<TYPE>(ptr_ + i * sizeof(TYPE));
	}

	MisalignedPtrBase(PTR ptr) noexcept : ptr_(ptr) {
	}
	MisalignedPtrBase operator++(int) noexcept {
		return MisalignedPtr(ptr_ + sizeof(TYPE));
	}
	MisalignedPtrBase operator--(int) noexcept {
		return MisalignedPtr(ptr_ - sizeof(TYPE));
	}
	MisalignedPtrBase operator+(difference_type d) noexcept {
		return MisalignedPtr(ptr_ + d * sizeof(TYPE));
	}
	MisalignedPtrBase operator-(difference_type d) noexcept {
		return MisalignedPtr(ptr_ - d * sizeof(TYPE));
	}

	MisalignedPtrBase &operator++() noexcept {
		ptr_ += sizeof(TYPE);
		return *this;
	}
	MisalignedPtrBase &operator--() noexcept {
		ptr_ -= sizeof(TYPE);
		return *this;
	}
	MisalignedPtrBase &operator+=(difference_type d) noexcept {
		ptr_ += d * sizeof(TYPE);
		return *this;
	}
	MisalignedPtrBase &operator-=(difference_type d) noexcept {
		ptr_ -= d * sizeof(TYPE);
		return *this;
	}

	bool operator==(MisalignedPtrBase const &other) noexcept {
		return ptr_ == other.ptr_;
	}
	bool operator!=(MisalignedPtrBase const &other) noexcept {
		return ptr_ != other.ptr_;
	}
};

template <class T>
class MisalignedPtr : public MisalignedPtrBase<T, data_ptr_t, MisalignedPtr<T>, MisalignedRef<T>> {};

template <class T>
class ConstMisalignedPtr : public MisalignedPtrBase<T, const_data_ptr_t, ConstMisalignedPtr<T>, ConstMisalignedRef<T>> {
};

template <class TYPE, class PTR, class pointer, class reference>
class StridedPtrBase {
private:
	PTR ptr;
	size_t stride;

public:
	using iterator_category = std::random_access_iterator_tag;
	using value_type = TYPE;
	using difference_type = std::ptrdiff_t;

	reference operator*() const noexcept {
		return {ptr};
	}
	reference operator[](std::size_t i) noexcept {
		return reference(ptr + i * stride);
	}
	value_type operator[](std::size_t i) const noexcept {
		return Load<TYPE>(ptr + i * stride);
	}

	StridedPtrBase(PTR ptr, size_t stride) noexcept : ptr(ptr), stride(stride) {
	}
	StridedPtrBase operator++(int) noexcept {
		return StridedPtrBase(ptr + stride, stride);
	}
	StridedPtrBase operator--(int) noexcept {
		return StridedPtrBase(ptr - stride, stride);
	}
	StridedPtrBase operator+(difference_type d) noexcept {
		return StridedPtrBase(ptr + d * stride, stride);
	}
	StridedPtrBase operator-(difference_type d) noexcept {
		return StridedPtrBase(ptr - d * stride, stride);
	}

	StridedPtrBase &operator++() noexcept {
		ptr += stride;
		return *this;
	}
	StridedPtrBase &operator--() noexcept {
		ptr -= stride;
		return *this;
	}
	StridedPtrBase &operator+=(difference_type d) noexcept {
		ptr += d * stride;
		return *this;
	}
	StridedPtrBase &operator-=(difference_type d) noexcept {
		ptr -= d * stride;
		return *this;
	}

	bool operator==(StridedPtrBase const &other) noexcept {
		return ptr == other.ptr;
	}
	bool operator!=(StridedPtrBase const &other) noexcept {
		return ptr != other.ptr;
	}
};

template <class T>
class StridedPtr : public StridedPtrBase<T, data_ptr_t, StridedPtr<T>, MisalignedRef<T>> {
public:
	StridedPtr(data_ptr_t ptr, size_t stride) noexcept
	    : StridedPtrBase<T, data_ptr_t, StridedPtr<T>, MisalignedRef<T>>(ptr, stride) {
	}
};

template <class T>
class ConstStridedPtr : public StridedPtrBase<T, const_data_ptr_t, ConstStridedPtr<T>, ConstMisalignedRef<T>> {
public:
	ConstStridedPtr(const_data_ptr_t ptr, size_t stride) noexcept
	    : StridedPtrBase<T, const_data_ptr_t, ConstStridedPtr<T>, ConstMisalignedRef<T>>(ptr, stride) {
	}
};

} // namespace core

} // namespace spatial