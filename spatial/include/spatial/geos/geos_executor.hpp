#pragma once

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace spatial {

namespace geos {

// Reflexive: Equals, Contains, Covers, CoveredBy, Intersects, Within
// Anti-reflexive: Disjoint
// Symmetric: Equals, Intersects, Crosses, Touches, Overlaps (and Disjoint? since Disjoint != Intersects)
// Transitive: Equals, Contains, Covers, CoveredBy, Within

// Optimize binary predicate helper which use prepared geometry when one of the arguments is a constant
// This is much more common than you would think, e.g. joins produce a lot of constant vectors.
typedef char (*GEOSBinaryPredicate)(GEOSContextHandle_t ctx, const GEOSGeometry *left, const GEOSGeometry *right);
typedef char (*GEOSPreparedBinaryPredicate)(GEOSContextHandle_t ctx, const GEOSPreparedGeometry *left,
                                            const GEOSGeometry *right);

struct GEOSExecutor {
	// Symmetric: left and right can be swapped
	// So we prepare either if one is constant
	static void ExecuteSymmetricPreparedBinary(GEOSFunctionLocalState &lstate, Vector &left, Vector &right, idx_t count,
	                                           Vector &result, GEOSBinaryPredicate normal,
	                                           GEOSPreparedBinaryPredicate prepared) {
		auto &ctx = lstate.ctx.GetCtx();

		if (left.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    right.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			auto &left_blob = FlatVector::GetData<string_t>(left)[0];
			auto left_geom = lstate.factory.Deserialize(left_blob);
			auto left_geos = lstate.ctx.FromGeometry(left_geom);
			auto left_prepared_geos = left_geos.Prepare();

			UnaryExecutor::Execute<string_t, bool>(right, result, count, [&](string_t &right_blob) {
				auto right_geometry = lstate.factory.Deserialize(right_blob);
				auto geos_right = lstate.ctx.FromGeometry(right_geometry);
				auto ok = prepared(ctx, left_prepared_geos.get(), geos_right.get());
				return ok == 1;
			});
		} else if (right.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		           left.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			auto &right_blob = FlatVector::GetData<string_t>(right)[0];
			auto right_geom = lstate.factory.Deserialize(right_blob);
			auto right_geos = lstate.ctx.FromGeometry(right_geom);
			auto right_prepared_geos = right_geos.Prepare();

			UnaryExecutor::Execute<string_t, bool>(left, result, count, [&](string_t &left_blob) {
				auto left_geometry = lstate.factory.Deserialize(left_blob);
				auto geos_left = lstate.ctx.FromGeometry(left_geometry);
				auto ok = prepared(ctx, right_prepared_geos.get(), geos_left.get());
				return ok == 1;
			});
		} else {
			BinaryExecutor::Execute<string_t, string_t, bool>(
			    left, right, result, count, [&](string_t &left_blob, string_t &right_blob) {
				    auto left_geometry = lstate.factory.Deserialize(left_blob);
				    auto right_geometry = lstate.factory.Deserialize(right_blob);
				    auto geos_left = lstate.ctx.FromGeometry(left_geometry);
				    auto geos_right = lstate.ctx.FromGeometry(right_geometry);
				    auto ok = normal(ctx, geos_left.get(), geos_right.get());
				    return ok == 1;
			    });
		}
	}

	// Non symmetric: left and right cannot be swapped
	// So we only prepare left if left is constant
	static void ExecuteNonSymmetricPreparedBinary(GEOSFunctionLocalState &lstate, Vector &left, Vector &right,
	                                              idx_t count, Vector &result, GEOSBinaryPredicate normal,
	                                              GEOSPreparedBinaryPredicate prepared) {
		auto &ctx = lstate.ctx.GetCtx();

		// Optimize: if one of the arguments is a constant, we can prepare it once and reuse it
		if (left.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    right.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			auto &left_blob = FlatVector::GetData<string_t>(left)[0];
			auto left_geom = lstate.factory.Deserialize(left_blob);
			auto left_geos = lstate.ctx.FromGeometry(left_geom);
			auto left_prepared_geos = left_geos.Prepare();

			UnaryExecutor::Execute<string_t, bool>(right, result, count, [&](string_t &right_blob) {
				auto right_geometry = lstate.factory.Deserialize(right_blob);
				auto geos_right = lstate.ctx.FromGeometry(right_geometry);
				auto ok = prepared(ctx, left_prepared_geos.get(), geos_right.get());
				return ok == 1;
			});
		} else {
			BinaryExecutor::Execute<string_t, string_t, bool>(
			    left, right, result, count, [&](string_t &left_blob, string_t &right_blob) {
				    auto left_geometry = lstate.factory.Deserialize(left_blob);
				    auto right_geometry = lstate.factory.Deserialize(right_blob);
				    auto geos_left = lstate.ctx.FromGeometry(left_geometry);
				    auto geos_right = lstate.ctx.FromGeometry(right_geometry);
				    auto ok = normal(ctx, geos_left.get(), geos_right.get());
				    return ok == 1;
			    });
		}
	}
};

} // namespace geos

} // namespace spatial