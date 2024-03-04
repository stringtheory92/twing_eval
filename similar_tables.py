import pandas as pd
import matplotlib.pyplot as plt

from tables_columns import main as tables_columns_table
from utils import (
    visualize_sim_ratio_bins,
    create_sim_ratio_bins_dataset,
    calculate_similarity_threshold,
    create_similar_tables_data,
)


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


def add_column_overlap_column(data):
    """
    Create dataset:

    | Table       | Columns                                                                 | ad_combined_overlap | ad_combined_sim_ratio | agg_daily_overlap | agg_daily_sim_ratio | ad_view_overlap | ad_view_sim_ratio | browser_overlap | browser_sim_ratio | ad_win_overlap | ad_win_sim_ratio | city_overlap | city_sim_ratio | brand_category_overlap | brand_category_sim_ratio | agg_monthly_v2_overlap | agg_monthly_v2_sim_ratio | brand_overlap | brand_sim_ratio | operating_system_overlap | operating_system_sim_ratio | agg_monthly_overlap | agg_monthly_sim_ratio | ad_request_overlap | ad_request_sim_ratio | ad_conversion_overlap | ad_conversion_sim_ratio | ad_click_overlap | ad_click_sim_ratio | creative_overlap | creative_sim_ratio | agg_hourly_overlap | agg_hourly_sim_ratio | region_overlap | region_sim_ratio |
    |-------------|-------------------------------------------------------------------------|---------------------|-----------------------|-------------------|---------------------|-----------------|-------------------|-----------------|-------------------|----------------|------------------|--------------|----------------|------------------------|--------------------------|------------------------|--------------------------|---------------|-----------------|--------------------------|----------------------------|---------------------|-----------------------|--------------------|----------------------|-----------------------|-------------------------|------------------|--------------------|------------------|--------------------|--------------------|----------------------|----------------|------------------|
    | ad_combined | ['postal_code', 'view', 'country_id', 'click', 'os_id']                 |
    | agg_daily   | ['postal_code', 'conversions', 'views', 'os_id', 'country_id']          |
    | ad_view     | ['ad_request_id', 'datetime']                                           |
    | browser     | ['id', 'name']                                                          |
    | ad_win      | ['creative_id', 'ad_request_id', 'cost', 'price', 'datetime']           |

    Rows: {table_name}
    Cols: {other_table_name}_overlap | {other_table_name}_sim_ratio

    Similarity will include PK/FK
    """

    modified_data = data.copy()

    # shorten table names for convenience
    # reconcile pk/fk col and ambiguous col names
    for index, row in modified_data.iterrows():
        table = row["table"].split(".")[-1]
        modified_data.at[index, "table"] = table
        for i, col in enumerate(row["columns"]):
            if table == "operating_system" and col == "id":
                modified_data.at[index, "columns"][i] = "os_id"
            elif col == "id":
                modified_data.at[index, "columns"][i] = f"{table}_id"
            elif col == "name":
                modified_data.at[index, "columns"][i] = f"{table}_name"

    # print("new data: ", modified_data)

    # create new dataset
    rows = []
    tables = modified_data["table"].to_list()

    for table in tables:
        row = {
            "table": table,
            "columns": modified_data.loc[
                modified_data["table"] == table, "columns"
            ].iloc[0],
        }

        for other_table in tables:
            if table == other_table:
                continue

            cols = row["columns"]
            other_cols = modified_data.loc[
                modified_data["table"] == other_table, "columns"
            ].iloc[0]

            overlap_cols = [col for col in cols if col in other_cols]
            sim_ratio = (len(overlap_cols) * 2) / (len(cols) + len(other_cols))

            row[f"{other_table}_overlap"] = overlap_cols
            row[f"{other_table}_sim_ratio"] = sim_ratio

        rows.append(row)

    overlap_data = pd.DataFrame(rows)
    overlap_data.to_csv("overlap_data.csv", index=False)

    # create sim_ratios dataset from mod results
    sim_ratios = pd.DataFrame({"table": overlap_data["table"]})

    for col in overlap_data.columns:
        if col.endswith("_sim_ratio"):
            other_table_name = col.split("_sim_ratio")[0]

            sim_ratios[other_table_name] = overlap_data[col]

    sim_ratios.to_csv("sim_ratios.csv", index=False)

    sim_ratio_bins_data, bins_count_data = create_sim_ratio_bins_dataset(sim_ratios)
    # visualize_sim_ratio_bins(sim_ratio_bins_data)
    threshold = calculate_similarity_threshold(bins_count_data)

    sim_tables = create_similar_tables_data(
        overlap_data, sim_ratio_bins_data, threshold
    )
    print("TABLE GROUPS: ", sim_tables)


# Hard-coded table grouping
def table_groups(data):
    """
    Produces lists of tables grouped by various criteria - Hard coded
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

    # AD TABLES
    ad_tables = []
    for _, row in table_col_data.iterrows():
        table = row["table"]
        columns = row["columns"]
        fkts = row["foreign_key_tables"]

        if "ad_request_id" in columns:
            ad_tables.append(table)

    print("ad tables: ", ad_tables)

    # AGG TABLES
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
    # table_groups(data)
    add_column_overlap_column(data)
