from refined import main as revert_aliases
import pandas as pd
import ast
import json
from collections import defaultdict


def extract_columns(row):
    """
    Extract columns from the specified columns (select, join, where, agg) of the transformed dataset.
    """
    columns = []
    columns_to_extract = [
        "select_columns",
        "join_columns",
        "where_columns",
        "agg_columns",
        "update_columns",
        "insert_columns",
    ]
    for col in columns_to_extract:
        if isinstance(row[col], list):
            columns.extend([column.split(".")[-1] for column in row[col]])
    return columns


def unique_tables(data):
    unique_tables = set()
    for _, row in data.iterrows():
        tables_list = ast.literal_eval(row["tables"])
        for table in tables_list:
            parts = table.split(" as ")
            if len(parts) == 2:
                table_name = parts[0]
            elif len(parts) == 1:
                print(f"(tables_columns.py) No alias found: {table}")
                table_name = table
            else:
                print(
                    f"(tables_columns.py) Unexpected format in 'tables' column: {table}"
                )
            unique_tables.add(table_name)
    return unique_tables


def generate_columns_per_table_dict(data, unique_tables):
    columns_to_scan = [
        "select_columns",
        "join_columns",
        "where_columns",
        "agg_columns",
        "update_columns",
        "insert_columns",
    ]

    table_columns = {table: set() for table in unique_tables}

    for _, row in data.iterrows():
        for table_name in unique_tables:
            for col in columns_to_scan:
                column_value = row[col]
                if isinstance(column_value, float) and pd.isna(column_value):
                    continue
                if isinstance(column_value, str):
                    columns = json.loads(column_value)
                elif isinstance(column_value, list):
                    columns = column_value
                else:
                    continue
                for column in columns:
                    if column.startswith(table_name + "."):
                        column_name = column.split(".")[-1]
                        table_columns[table_name].add(column_name)

    return table_columns


def create_columns_dataset(data):
    """
    Create a new dataset containing unique tables and their columns.
    """

    table_set = unique_tables(data)
    table_columns = generate_columns_per_table_dict(data, table_set)

    table_columns_aggregated = defaultdict(list)
    for table, columns in table_columns.items():
        table_columns_aggregated[table].extend(columns)

    rows = []
    for table, columns in table_columns_aggregated.items():
        rows.append({"table": table, "columns": columns})

    columns_data = pd.DataFrame(rows)

    return columns_data


def main():
    refined_data = revert_aliases()
    columns_dataset = create_columns_dataset(refined_data)

    columns_dataset.to_csv("tables_columns.csv", index=False)

    print("Columns Dataset:")
    print(columns_dataset)
    return columns_dataset


if __name__ == "__main__":
    main()
