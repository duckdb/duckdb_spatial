#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct MathUtil {
	static string format_coord(double d);
	static string format_coord(double x, double y);
	static string format_coord(double x, double y, double z);
	static string format_coord(double x, double y, double z, double m);

	static inline float DoubleToFloatDown(double d) {
		if (d > static_cast<double>(std::numeric_limits<float>::max())) {
			return std::numeric_limits<float>::max();
		}
		if (d <= static_cast<double>(std::numeric_limits<float>::lowest())) {
			return std::numeric_limits<float>::lowest();
		}

		auto f = static_cast<float>(d);
		if (static_cast<double>(f) <= d) {
			return f;
		}
		return std::nextafter(f, std::numeric_limits<float>::lowest());
	}

	static inline float DoubleToFloatUp(double d) {
		if (d >= static_cast<double>(std::numeric_limits<float>::max())) {
			return std::numeric_limits<float>::max();
		}
		if (d < static_cast<double>(std::numeric_limits<float>::lowest())) {
			return std::numeric_limits<float>::lowest();
		}

		auto f = static_cast<float>(d);
		if (static_cast<double>(f) >= d) {
			return f;
		}
		return std::nextafter(f, std::numeric_limits<float>::max());
	}
};

} // namespace core

} // namespace spatial