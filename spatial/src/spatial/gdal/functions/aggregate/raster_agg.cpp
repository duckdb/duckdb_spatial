#include "spatial/gdal/functions/raster_agg.hpp"

#include "gdal_priv.h"

namespace spatial {

namespace gdal {

unique_ptr<FunctionData> BindRasterAggOperation(ClientContext &context, AggregateFunction &function,
                                                vector<unique_ptr<Expression>> &arguments) {

	std::vector<std::string> options;

	// First arg is a raster, next ones are optional arguments
	for (std::size_t i = 1; i < arguments.size(); i++) {
		auto &arg = arguments[i];

		if (arg->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arg->IsFoldable()) {
			throw BinderException("raster_agg: arguments must be constant");
		}
		if (arg->alias == "options" || arg->return_type.id() == LogicalTypeId::LIST) {

			Value param_list = ExpressionExecutor::EvaluateScalar(context, *arg);
			auto &params = ListValue::GetChildren(param_list);

			for (std::size_t j = 0; j < params.size(); j++) {
				const auto &param_value = params[j];
				const auto option = param_value.ToString();
				options.push_back(option);
			}
		} else {
			throw BinderException(StringUtil::Format("raster_agg: Unknown argument '%s'", arg->alias.c_str()));
		}
	}

	return make_uniq<RasterAggBindData>(context, options);
}

} // namespace gdal

} // namespace spatial
