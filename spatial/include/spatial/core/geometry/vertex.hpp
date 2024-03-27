#pragma once

#include "spatial/common.hpp"

namespace spatial {

namespace core {

enum class VertexType : uint8_t { XY, XYZ, XYM, XYZM };

struct VertexXY {
	static const constexpr VertexType TYPE = VertexType::XY;
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = false;
	static const constexpr bool HAS_M = false;

	double x;
	double y;
};

struct VertexXYZ {
	static const constexpr VertexType TYPE = VertexType::XYZ;
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = true;
	static const constexpr bool HAS_M = false;

	double x;
	double y;
	double z;
};

struct VertexXYM {
	static const constexpr VertexType TYPE = VertexType::XYM;
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = false;
	static const constexpr bool HAS_M = true;

	double x;
	double y;
	double m;
};

struct VertexXYZM {
	static const constexpr VertexType TYPE = VertexType::XYZM;
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = true;
	static const constexpr bool HAS_M = true;

	double x;
	double y;
	double z;
	double m;
};

} // namespace core

} // namespace spatial
