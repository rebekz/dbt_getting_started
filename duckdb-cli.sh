#!/bin/bash
# DuckDB CLI wrapper using uv venv
# Usage: ./duckdb-cli.sh [database_file] [sql_query]

cd "$(dirname "$0")"

if [ -n "$2" ]; then
    # If SQL query provided, run it
    uv run python -c "import duckdb; con=duckdb.connect('$1'); result=con.execute('''$2''').fetchdf(); print(result.to_string(index=False)); con.close()"
elif [ -n "$1" ]; then
    # If only database file provided, open interactive shell
    uv run python -m duckdb "$1"
else
    # No arguments, show usage
    echo "Usage:"
    echo "  ./duckdb-cli.sh <database_file>              # Interactive shell"
    echo "  ./duckdb-cli.sh <database_file> 'SQL query'  # Run query"
    echo ""
    echo "Examples:"
    echo "  ./duckdb-cli.sh my_first_project/my_dbt.duckdb"
    echo "  ./duckdb-cli.sh my_first_project/my_dbt.duckdb 'SHOW TABLES'"
    exit 1
fi

