import pandas as pd
import ast

from tables_columns import main as tables_columns_table


data = tables_columns_table()


def add_foreign_key_tables_column(data):
    foreign_key_tables_list = []

    for _, row in data.iterrows():
        potential_foreign_tables = set()
        confirmed_foreign_tables = []

        for column in row["columns"]:
            if column.endswith("_id"):
                potential_foreign_table = column[:-3]
                potential_foreign_tables.add(potential_foreign_table)

        for _, other_row in data.iterrows():
            full_table_name = other_row["table"]
            table_name = other_row["table"].split(".")[-1]

            for potential_foreign_table in potential_foreign_tables:
                if potential_foreign_table == table_name:
                    confirmed_foreign_tables.append(table_name)

        foreign_key_tables_list.append(confirmed_foreign_tables)

    data["foreign_key_tables"] = [foreign_key_tables_list[i] for i in range(len(data))]
    data.to_csv("foreign_key_tables.csv", index=False)
    print(data)
    return data


if __name__ == "__main__":

    add_foreign_key_tables_column(data)
