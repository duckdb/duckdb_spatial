#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

//! Supported Pixel data types (GDALDataType).
typedef enum
{
	Unknown = 0,    /**< Unknown or unspecified type     */
	Byte = 1,       /**< Eight bit unsigned integer      */
	Int8 = 14,      /**< 8-bit signed integer            */
	UInt16 = 2,     /**< Sixteen bit unsigned integer    */
	Int16 = 3,      /**< Sixteen bit signed integer      */
	UInt32 = 4,     /**< Thirty two bit unsigned integer */
	Int32 = 5,      /**< Thirty two bit signed integer   */
	UInt64 = 12,    /**< 64 bit unsigned integer         */
	Int64 = 13,     /**< 64 bit signed integer           */
	Float32 = 6,    /**< Thirty two bit floating point   */
	Float64 = 7,    /**< Sixty four bit floating point   */
	CInt16 = 8,     /**< Complex Int16                   */
	CInt32 = 9,     /**< Complex Int32                   */
	CFloat32 = 10,  /**< Complex Float32                 */
	CFloat64 = 11,  /**< Complex Float64                 */
	TypeCount = 15  /**< maximum type # + 1              */
} PixelType;

//! Returns the name of given PixelType
std::string GetPixelTypeName(const PixelType& pixel_type);

//! Supported Types of color interpretation for raster bands (GDALColorInterp).
typedef enum
{
	Undefined = 0,      /**< Undefined                             */
	GrayIndex = 1,      /**< Greyscale                             */
	PaletteIndex = 2,   /**< Paletted (see associated color table) */
	RedBand = 3,        /**< Red band of RGBA image                */
	GreenBand = 4,      /**< Green band of RGBA image              */
	BlueBand = 5,       /**< Blue band of RGBA image               */
	AlphaBand = 6,      /**< Alpha (0=transparent, 255=opaque)     */
	HueBand = 7,        /**< Hue band of HLS image                 */
	SaturationBand = 8, /**< Saturation band of HLS image          */
	LightnessBand = 9,  /**< Lightness band of HLS image           */
	CyanBand = 10,      /**< Cyan band of CMYK image               */
	MagentaBand = 11,   /**< Magenta band of CMYK image            */
	YellowBand = 12,    /**< Yellow band of CMYK image             */
	BlackBand = 13,     /**< Black band of CMYK image              */
	YCbCr_YBand = 14,   /**< Y Luminance                           */
	YCbCr_CbBand = 15,  /**< Cb Chroma                             */
	YCbCr_CrBand = 16   /**< Cr Chroma                             */
} ColorInterp;

//! Returns the name of given ColorInterp
std::string GetColorInterpName(const ColorInterp& color_interp);

} // namespace gdal

} // namespace spatial
