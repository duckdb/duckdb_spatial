import os
import json

get_spatial_functions_sql = """
SELECT
    json({ 
        name: function_name,
        signatures: signatures,
        tags: func_tags,
        description: description,
        example: example
    })
FROM (
    SELECT
        function_type AS type,
        function_name,
        list({
            return: return_type,
            params: list_zip(parameters, parameter_types)::STRUCT(name VARCHAR, type VARCHAR)[]
        }) as signatures,
        any_value(tags) AS func_tags,
        any_value(description) AS description,
        any_value(example) AS example
    FROM duckdb_functions() as funcs
    WHERE function_type = '$FUNCTION_TYPE$'
    GROUP BY function_name, function_type
    HAVING func_tags['ext'] = ['spatial']
    ORDER BY function_name
);
"""

def get_functions(function_type = 'scalar'):
    functions = []
    for line in os.popen("./build/debug/duckdb -list -noheader -c \"" + get_spatial_functions_sql.replace('$FUNCTION_TYPE$', function_type) + "\"").readlines():
        functions.append(json.loads(line))
    return functions

def write_table_of_contents(f, functions):
    f.write('| Function | Summary |\n')
    f.write('| --- | --- |\n')
    for function in functions:
        # Summary is the first line of the description
        summary = function['description'].split('\n')[0]
        f.write(f"| [{function['name']}](#{to_kebab_case(function['name'])}) | {summary} |\n")


def to_kebab_case(name):
    return name.replace(" ", "-").lower()

def main():
    with open("./docs/functions.md", "w") as f:

        f.write("# DuckDB Spatial Function Reference\n\n")

        print("Collecting functions")

        aggregate_functions = get_functions('aggregate')
        scalar_functions = get_functions('scalar')
        table_functions = get_functions('table')

        # Write function index
        f.write("## Function Index \n")
        f.write("**[Scalar Functions](#scalar-functions)**\n\n")
        write_table_of_contents(f, scalar_functions)
        f.write("\n")
        f.write("**[Aggregate Functions](#aggregate-functions)**\n\n")
        write_table_of_contents(f, aggregate_functions)
        f.write("\n")
        f.write("**[Table Functions](#table-functions)**\n\n")
        write_table_of_contents(f, table_functions)
        f.write("\n")
        f.write("----\n\n")

        # Write basic functions
        for func_set in [('Scalar Functions', scalar_functions), ('Aggregate Functions', aggregate_functions)]:
            f.write(f"## {func_set[0]}\n\n")
            set_name = func_set[0]
            for function in func_set[1]:
                f.write(f"### {function['name']}\n\n")
                summary = function['description'].split('\n')[0]
                f.write(f"_{summary}_\n\n")

                f.write("#### Signature\n\n")
                f.write("```sql\n")
                for signature in function['signatures']:
                    param_list = ", ".join([f"{param['name']} {param['type']}" for param in signature['params']])
                    f.write(f"{signature['return']} {function['name']} ({param_list})\n")
                f.write("```\n\n")

                if function['description']:
                    f.write("#### Description\n\n")
                    f.write(function['description'])
                    f.write("\n\n")
                else:
                    print(f"No description for {function['name']}")

                if function['example']:
                    f.write("#### Example\n\n")
                    f.write("```sql\n")
                    f.write(function['example'])
                    f.write("\n```\n\n")
                else:
                    print(f"No example for {function['name']}")
                f.write("\n\n")

        # Write table functions
        f.write("## Table Functions\n\n")
        for function in table_functions:
            f.write(f"### {function['name']}\n\n")
            summary = function['description'].split('\n')[0]
            f.write(f"_{summary}_\n\n")

            f.write("#### Signature\n\n")
            f.write("```sql\n")
            for signature in function['signatures']:
                param_list = ", ".join([f"{param['name']} {param['type']}" for param in signature['params']])
                f.write(f"{function['name']} ({param_list})\n")
            f.write("```\n\n")

            if function['description']:
                f.write("#### Description\n\n")
                f.write(function['description'])
                f.write("\n\n")
            else:
                print(f"No description for {function['name']}")

            if function['example']:
                f.write("#### Example\n\n")
                f.write("```sql\n")
                f.write(function['example'])
                f.write("\n```\n\n")
            else:
                print(f"No example for {function['name']}")
            f.write("\n\n")

if __name__ == "__main__":
    main()
