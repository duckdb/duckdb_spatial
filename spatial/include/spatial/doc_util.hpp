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

        duckdb::vector<duckdb::Value> keys;
        duckdb::vector<duckdb::Value> values;
        for(size_t i = 0; i < N; i++) {
            keys.push_back(duckdb::Value(tags[i].key));
            values.push_back(duckdb::Value(tags[i].value));
        }

        auto tag_map = duckdb::Value::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, keys, values);
        AddDocumentation(db, function_name, description, example, tag_map);
    }
};

}