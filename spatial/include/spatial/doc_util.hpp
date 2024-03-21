#pragma once
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

namespace spatial {

struct DocTag {
    const char* key;
    const char* value;
};

struct DocUtil {
    static void AddDocumentation(duckdb::DatabaseInstance &db, const char* function_name, const char* description, const char* example, const duckdb::Value &comment);

    // Abuse adding tags as a comment
    template<size_t N>
    static void AddDocumentation(duckdb::DatabaseInstance &db, const char* function_name, const char* description, const char* example
        , const DocTag (& tags) [N]
    ) {
        auto kv_type = duckdb::LogicalType::STRUCT({{"key", duckdb::LogicalType::VARCHAR}, {"value", duckdb::LogicalType::VARCHAR}});
        duckdb::vector<duckdb::Value> tag_values;
        for(size_t i = 0; i < N; i++) {
            auto &tag = tags[i];
            tag_values.push_back(duckdb::Value::STRUCT(kv_type, {duckdb::Value(tag.key), duckdb::Value(tag.value)}));
        }
        AddDocumentation(db, function_name, description, example, duckdb::Value::LIST(kv_type, tag_values));
    }
};

}