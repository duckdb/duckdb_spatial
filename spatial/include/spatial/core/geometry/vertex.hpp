#pragma once

#include "spatial/common.hpp"

namespace spatial {

namespace core {

template <class T>
struct PointXY {
	using VALUE_TYPE = T;
	static constexpr idx_t SIZE = 2;

public:
	T x;
	T y;

public:
	PointXY() = default;
	PointXY(const T &x_p, const T &y_p) : x(x_p), y(y_p) {
	}
	explicit PointXY(const T &val_p) : x(val_p), y(val_p) {
	}

	T &operator[](const idx_t i) {
		D_ASSERT(i < 2);
		return i == 0 ? x : y;
	}
	T operator[](const idx_t i) const {
		D_ASSERT(i < 2);
		return i == 0 ? x : y;
	}

	PointXY operator+(const PointXY &other) const {
		return PointXY(x + other.x, y + other.y);
	}

	PointXY operator-(const PointXY &other) const {
		return PointXY(x - other.x, y - other.y);
	}

	PointXY operator*(const T &factor) const {
		return PointXY(x * factor, y * factor);
	}

	PointXY operator/(const T &factor) const {
		return PointXY(x / factor, y / factor);
	}

	bool ApproxEqualTo(const PointXY &other) const {
		return std::abs(x - other.x) < 1e-6 && std::abs(y - other.y) < 1e-6;
	}

	bool operator==(const PointXY &other) const {
		return x == other.x && y == other.y;
	}

	bool operator!=(const PointXY &other) const {
		return x != other.x || y != other.y;
	}
};

template <class T>
struct PointXYZ : PointXY<T> {
public:
	using VALUE_TYPE = T;
	static constexpr idx_t SIZE = 3;

public:
	using PointXY<T>::x;
	using PointXY<T>::y;
	T z;

public:
	PointXYZ() = default;
	PointXYZ(const T &x_p, const T &y_p, const T &z_p) : PointXY<T>(x_p, y_p), z(z_p) {
	}
	explicit PointXYZ(const T &val_p) : PointXY<T>(val_p), z(val_p) {
	}

	T &operator[](const idx_t i) {
		D_ASSERT(i < 3);
		return i == 0 ? x : i == 1 ? y : z;
	}

	T operator[](const idx_t i) const {
		D_ASSERT(i < 3);
		return i == 0 ? x : i == 1 ? y : z;
	}
};

template <class T>
struct PointXYM : PointXY<T> {
public:
	using VALUE_TYPE = T;
	static constexpr idx_t SIZE = 3;

public:
	using PointXY<T>::x;
	using PointXY<T>::y;
	T m;

public:
	PointXYM() = default;
	PointXYM(const T &x_p, const T &y_p, const T &m_p) : PointXY<T>(x_p, y_p), m(m_p) {
	}
	explicit PointXYM(const T &val_p) : PointXY<T>(val_p), m(val_p) {
	}

	T &operator[](const idx_t i) {
		D_ASSERT(i < 3);
		return i == 0 ? x : i == 1 ? y : m;
	}
	T operator[](const idx_t i) const {
		D_ASSERT(i < 3);
		return i == 0 ? x : i == 1 ? y : m;
	}
};

template <class T>
struct PointXYZM : PointXYZ<T> {
public:
	using VALUE_TYPE = T;
	static constexpr idx_t SIZE = 4;

public:
	using PointXYZ<T>::x;
	using PointXYZ<T>::y;
	using PointXYZ<T>::z;
	T m;

public:
	PointXYZM() = default;
	PointXYZM(const T &x_p, const T &y_p, const T &z_p, const T &m_p) : PointXYZ<T>(x_p, y_p, z_p), m(m_p) {
	}
	explicit PointXYZM(const T &val_p) : PointXYZ<T>(val_p), m(val_p) {
	}

	T &operator[](const idx_t i) {
		D_ASSERT(i < 4);
		return i == 0 ? x : i == 1 ? y : i == 2 ? z : m;
	}

	T operator[](const idx_t i) const {
		D_ASSERT(i < 4);
		return i == 0 ? x : i == 1 ? y : i == 2 ? z : m;
	}
};

// TODO: Deprecate these for the generic PointXY, PointXYZ, PointXYM, PointXYZM instead
enum class VertexType : uint8_t { XY, XYZ, XYM, XYZM };
struct VertexXY : public PointXY<double> {
	static const constexpr VertexType TYPE = VertexType::XY;
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = false;
	static const constexpr bool HAS_M = false;

	VertexXY() = default;
	explicit VertexXY(double val) : PointXY<double>(val) {
	}
	VertexXY(double x, double y) : PointXY<double>(x, y) {
	}
};

struct VertexXYZ : public PointXYZ<double> {
	static const constexpr VertexType TYPE = VertexType::XYZ;
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = true;
	static const constexpr bool HAS_M = false;

	VertexXYZ() = default;
	explicit VertexXYZ(double val) : PointXYZ<double>(val) {
	}
	VertexXYZ(double x, double y, double z) : PointXYZ<double>(x, y, z) {
	}
};

struct VertexXYM : public PointXYM<double> {
	static const constexpr VertexType TYPE = VertexType::XYM;
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = false;
	static const constexpr bool HAS_M = true;

	VertexXYM() = default;
	explicit VertexXYM(double val) : PointXYM<double>(val) {
	}
	VertexXYM(double x, double y, double m) : PointXYM<double>(x, y, m) {
	}
};

struct VertexXYZM : public PointXYZM<double> {
	static const constexpr VertexType TYPE = VertexType::XYZM;
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = true;
	static const constexpr bool HAS_M = true;

	VertexXYZM() = default;
	explicit VertexXYZM(double val) : PointXYZM<double>(val) {
	}
	VertexXYZM(double x, double y, double z, double m) : PointXYZM<double>(x, y, z, m) {
	}
};

} // namespace core

} // namespace spatial
