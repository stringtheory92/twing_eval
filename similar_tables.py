import pandas as pd
from pandasql import sqldf
import ast

from tables_columns import main as tables_columns_table


data = tables_columns_table()


def add_foreign_key_tables_column(data):
    """
    Based on naming convention [table].id => table_2.[table]_id
    Returns tables_columns_table with additional column
    """
    foreign_key_tables_list = []

    for _, row in data.iterrows():
        potential_foreign_tables = set()
        confirmed_foreign_tables = []

        for column in row["columns"]:
            if column.endswith("_id"):
                potential_foreign_table = column[:-3]
                potential_foreign_tables.add(potential_foreign_table)

        for _, other_row in data.iterrows():
            # toggle full table paths or only names
            full_table_name = other_row["table"]
            table_name = other_row["table"].split(".")[-1]

            for potential_foreign_table in potential_foreign_tables:
                if potential_foreign_table == table_name:
                    confirmed_foreign_tables.append(table_name)

        foreign_key_tables_list.append(confirmed_foreign_tables)

    data["foreign_key_tables"] = [foreign_key_tables_list[i] for i in range(len(data))]
    data.to_csv("foreign_key_tables.csv", index=False)

    return data


def table_groups(data):
    """
    Produces lists of tables grouped by various criteria
    """
    table_col_data = add_foreign_key_tables_column(data)

    # REFERENCE TABLES
    reference_tables = []
    for _, row in table_col_data.iterrows():
        table = row["table"]
        columns = row["columns"]
        fkts = row["foreign_key_tables"]

        if len(fkts) == 0:
            reference_tables.append(table)

    print("ref tables: ", reference_tables)

    # AD TABLES
    ad_tables = []
    for _, row in table_col_data.iterrows():
        table = row["table"]
        columns = row["columns"]
        fkts = row["foreign_key_tables"]

        if "ad_request_id" in columns:
            ad_tables.append(table)

    print("ad tables: ", ad_tables)

    # Agg Tables
    agg_tables = []
    for _, row in table_col_data.iterrows():
        table = row["table"]
        columns = row["columns"]
        fkts = row["foreign_key_tables"]

        agg_fkts = {"browser", "region", "city", "brand", "creative"}
        if len(fkts) == 5:
            is_agg_table = True
            for fkt in agg_fkts:
                if fkt not in fkts:
                    is_agg_table = False
            if is_agg_table == True:
                agg_tables.append(table)

    print("agg tables: ", agg_tables)


if __name__ == "__main__":

    # add_foreign_key_tables_column(data)
    table_groups(data)
