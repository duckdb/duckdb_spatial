#include "spatial/core/util/math.hpp"

namespace spatial {

namespace core {

// We've got this exposed upstream, we just need to wait for the next release
extern "C" int geos_d2sfixed_buffered_n(double f, uint32_t precision, char *result);

string MathUtil::format_coord(double d) {
	char buf[25];
	auto len = geos_d2sfixed_buffered_n(d, 15, buf);
	buf[len] = '\0';
	return string {buf};
}

string MathUtil::format_coord(double x, double y) {
	char buf[51];
	auto res_x = geos_d2sfixed_buffered_n(x, 15, buf);
	buf[res_x++] = ' ';
	auto res_y = geos_d2sfixed_buffered_n(y, 15, buf + res_x);
	buf[res_x + res_y] = '\0';
	return string {buf};
}

string MathUtil::format_coord(double x, double y, double zm) {
	char buf[76];
	auto res_x = geos_d2sfixed_buffered_n(x, 15, buf);
	buf[res_x++] = ' ';
	auto res_y = geos_d2sfixed_buffered_n(y, 15, buf + res_x);
	buf[res_x + res_y++] = ' ';
	auto res_zm = geos_d2sfixed_buffered_n(zm, 15, buf + res_x + res_y);
	buf[res_x + res_y + res_zm] = '\0';
	return string {buf};
}

string MathUtil::format_coord(double x, double y, double z, double m) {
	char buf[101];
	auto res_x = geos_d2sfixed_buffered_n(x, 15, buf);
	buf[res_x++] = ' ';
	auto res_y = geos_d2sfixed_buffered_n(y, 15, buf + res_x);
	buf[res_x + res_y++] = ' ';
	auto res_z = geos_d2sfixed_buffered_n(z, 15, buf + res_x + res_y);
	buf[res_x + res_y + res_z++] = ' ';
	auto res_m = geos_d2sfixed_buffered_n(m, 15, buf + res_x + res_y + res_z);
	buf[res_x + res_y + res_z + res_m] = '\0';
	return string {buf};
}

} // namespace core

} // namespace spatial