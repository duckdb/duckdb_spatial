#include "spatial/gdal/types.hpp"

#include "gdal_priv.h"

namespace spatial {

namespace gdal {

std::string GetPixelTypeName(const PixelType &pixel_type) {
	switch (pixel_type) {
	case Byte:
		return "Byte";
	case Int8:
		return "Int8";
	case UInt16:
		return "UInt16";
	case Int16:
		return "Int16";
	case UInt32:
		return "UInt32";
	case Int32:
		return "Int32";
	case UInt64:
		return "UInt64";
	case Int64:
		return "Int64";
	case Float32:
		return "Float32";
	case Float64:
		return "Float64";
	case CInt16:
		return "CInt16";
	case CInt32:
		return "CInt32";
	case CFloat32:
		return "CFloat32";
	case CFloat64:
		return "CFloat64";
	case TypeCount:
	case Unknown:
	default:
		return "Unknown";
	}
}

std::string GetColorInterpName(const ColorInterp &color_interp) {
	switch (color_interp) {
	case Undefined:
		return "Undefined";
	case GrayIndex:
		return "Greyscale";
	case PaletteIndex:
		return "Paletted";
	case RedBand:
		return "Red";
	case GreenBand:
		return "Green";
	case BlueBand:
		return "Blue";
	case AlphaBand:
		return "Alpha";
	case HueBand:
		return "Hue";
	case SaturationBand:
		return "Saturation";
	case LightnessBand:
		return "Lightness";
	case CyanBand:
		return "Cyan";
	case MagentaBand:
		return "Magenta";
	case YellowBand:
		return "Yellow";
	case BlackBand:
		return "Black";
	case YCbCr_YBand:
		return "YLuminance";
	case YCbCr_CbBand:
		return "CbChroma";
	case YCbCr_CrBand:
		return "CrChroma";
	default:
		return "Unknown";
	}
}

std::string GetResampleAlgName(const ResampleAlg &resample_alg) {
	switch (resample_alg) {
	case NearestNeighbour:
		return "NearestNeighbour";
	case Bilinear:
		return "Bilinear";
	case Cubic:
		return "Cubic";
	case CubicSpline:
		return "CubicSpline";
	case Lanczos:
		return "Lanczos";
	case Average:
		return "Average";
	case Mode:
		return "Mode";
	case Max:
		return "Maximum";
	case Min:
		return "Minimun";
	case Med:
		return "Median";
	case Q1:
		return "Quartile1";
	case Q3:
		return "Quartile3";
	case Sum:
		return "Sum";
	case RMS:
		return "RootMeanSquare";
	default:
		return "Unknown";
	}
}

} // namespace gdal

} // namespace spatial
