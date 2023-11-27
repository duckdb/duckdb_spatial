SELECT * FROM duckdb_functions() WHERE function_name LIKE 'st\_%' ESCAPE '\';

-- Get all scalars
COPY (
SELECT
    'scalar_function' as "type",
    lower(function_name) as id,
    function_name as title, 
    list( {'returns': return_type, 'parameters': parameter_types} ) as signatures
FROM duckdb_functions() 
WHERE function_name LIKE 'ST\_%' ESCAPE '\' AND function_type = 'scalar'
GROUP BY function_name
) TO 'scalar_functions.json';

-- Get all aggregates
COPY (
SELECT
    'aggregate_function' as "type",
    lower(function_name) as id,
    function_name as title, 
    list( {'returns': return_type, 'parameters': parameter_types} ) as signatures
FROM duckdb_functions() 
WHERE function_name LIKE 'ST\_%' ESCAPE '\' AND function_type = 'aggregate'
GROUP BY function_name
) TO 'aggregate_functions.json';

--- Get all table funcs
COPY (
SELECT
    'table_function' as "type",
    lower(function_name) as id,
    function_name as title, 
    list( {'returns': return_type, 'parameters': parameter_types} ) as signatures
FROM duckdb_functions() 
WHERE function_name LIKE 'ST\_%' ESCAPE '\' AND function_type = 'table'
GROUP BY function_name
) TO 'table_functions.json';