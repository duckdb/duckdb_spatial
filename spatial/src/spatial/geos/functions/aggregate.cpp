
#include "spatial/common.hpp"
#include "spatial/geos/functions/aggregate.hpp"

namespace spatial {

namespace geos {

/*
struct UnionState {
    geos::geom::Geometry::Ptr geom;
    bool is_set;
};

struct UnionOperation {
    template <class STATE>
    static void Initialize(STATE *state) {
        state->is_set = false;
        // Since the state is initialized by a memset(?) this unique ptr will point to garbage
        // so we cant just move assign nullptr since it will try to free the old garbage ptr.
        // therefore we manually release first to resign ownership of garbage and then initialize to nullptr
        state->geom.release();
        state->geom = nullptr;
    }

    template <class INPUT_TYPE, class STATE, class OP>
    static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {

        if (mask.RowIsValid(idx) == false) {
            return;
        }
        auto &input_blob = input[idx];
        auto input_data = (const unsigned char *)(input_blob.GetDataUnsafe());
        auto input_size = input_blob.GetSize();

        geos::io::WKBReader reader;

        if (!state->is_set) {
            state->is_set = true;
            auto wkb = reader.read(input_data, input_size);
            auto wkb_ptr = wkb.release();
            state->geom.reset(wkb_ptr);
        } else {
            state->geom = move(state->geom->Union(reader.read(input_data, input_size).get()));
        }
    }

    template <class STATE, class OP>
    static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
        if (!source.is_set) {
            return;
        }
        if (!target->is_set) {
            target->geom = source.geom->clone(); // TODO: There should be some way to avoid this clone?
            target->is_set = source.is_set;
        } else {
            target->geom = target->geom->Union(source.geom.get());
        }
    }

    template <class T, class STATE>
    static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
        if (!state->is_set) {
            mask.SetInvalid(idx);
        } else {
            geos::io::WKBWriter writer;
            writer.setIncludeSRID(true);
            std::stringstream buf;
            writer.write(*state->geom, buf);
            target[idx] = StringVector::AddStringOrBlob(result, buf.str());
        }
    }

    template <class INPUT_TYPE, class STATE, class OP>
    static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
                                  ValidityMask &mask, idx_t count) {

        // TODO: we can use geos to create a union from a list of geometries
        for (idx_t i = 0; i < count; i++) {
            Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
        }
    }

    static bool IgnoreNull() {
        return true;
    }
};

/*
static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
                                                        vector<unique_ptr<Expression>> &arguments) {
    // TODO: we should be able to create a index here?
    return nullptr;
}
*/

void GeosAggregateFunctions::Register(ClientContext &context) {
	/*
	auto geom_func = AggregateFunction::UnaryAggregate<UnionState, string_t, string_t, UnionOperation>(
	    GeoTypes::WKB_GEOMETRY, GeoTypes::WKB_GEOMETRY);

	geom_func.name = "st_union_agg";

	CreateAggregateFunctionInfo info(geom_func);
	info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	Catalog::GetSystemCatalog(context).CreateFunction(context, &info);
	*/
}

} // namespace spatials

} // namespace spatial