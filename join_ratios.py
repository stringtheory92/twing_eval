# 1/27: Currently the script takes a sql query string and returns a list of tables used in the FROM clause and their aliases. If the FROM contains subqueries, it replaces each subquery with the table used in the subquery and then runs the usual operation.

import re
import pandas as pd

from refined import main as revert_aliases
from tables_columns import main as tables_columns_table


def main():
    refined_table = revert_aliases()

    # Test SQL queries
    queries = [
        # "SELECT ymd, region_id, SUM(spend) AS total_spend, SUM(cost) AS total_cost FROM agg_monthly GROUP BY ymd, region_id;",
        # "SELECT * FROM public.agg_hourly WHERE ymd = '2024-02-16' AND hour = 9;",
        "SELECT * FROM public.agg_daily srt LEFT JOIN (SELECT id FROM public.brand WHERE name = 'A') A ON srt.brand_id = A.id LEFT JOIN (SELECT id FROM public.brand WHERE name = 'B') B ON srt.brand_id = B.id LEFT JOIN (SELECT id FROM public.brand WHERE name = 'C') C ON srt.brand_id = C.id GROUP BY srt.ymd;",
        # """SELECT brt.ymd, r.name AS region_name, b.name AS brand_name,
        #       os.name AS operating_system,
        #       br.name AS browser_name,
        #       SUM(brt.impressions) AS total_impressions,
        #       SUM(brt.clicks) AS total_clicks,
        #       AVG(brt.views) AS avg_views,
        #       SUM(brt.conversions) AS total_conversions,
        #       SUM(brt.spend) AS total_spend,
        #       (SUM(brt.clicks) / NULLIF(SUM(brt.impressions), 0)) * 100 AS click_through_rate_percentage
        #   FROM public.agg_daily brt
        #   JOIN public.region r ON brt.region_id = r.id
        #   JOIN public.brand b ON brt.brand_id = b.id
        #   JOIN public.browser br ON brt.browser_id = br.id
        #   JOIN public.operating_system os ON brt.os_id = os.id
        #   GROUP BY brt.ymd, r.name, b.name, os.name, br.name
        #   ORDER BY brt.ymd, total_impressions DESC;""",
    ]

    def extract_from_clause_no_subquery(query):
        """
        If no subqueries exist in FROM clause, this regex extracts table/alias names
        Returns Table/Alias DF
        """
        from_pattern = re.compile(
            r"FROM\s+(.+?)(?=\s+(?:GROUP\s+BY|HAVING|ORDER\s+BY|WHERE|$))",
            re.IGNORECASE | re.DOTALL,
        )
        matches = from_pattern.search(query)

        if matches:
            query = matches.group(1)
            first_table_match = re.search(
                r"^[^a-zA-Z]*(\b[a-zA-Z_.]+\b)\s+(?:AS\s+)?([a-zA-Z_]+)?(?=\s)", query
            )
            first_table = (
                (first_table_match.group(1), first_table_match.group(2))
                if first_table_match
                else None
            )
            tables = re.findall(
                r"\bJOIN\s+([a-zA-Z_.]+)\s+(?:AS\s+)?([a-zA-Z_]+)?",
                query,
            )
            if first_table:
                tables.insert(0, first_table)

            print("tables: ", tables)

            if tables:
                data = {
                    "Table": [table[0] for table in tables],
                    "Alias": [alias[1] for alias in tables],
                }
                return pd.DataFrame(data)
        return None

    def extract_from_clause_with_subquery(query, query_parts, subquery_indices):
        """
        finds [(subquery_start_index, subquery_end_index, table_name_index, alias_name_index)]
        replaces subquery with subquery_table_name and alias_name
        calls extract_from_clause_no_subquery with altered query
        Returns Table/Alias DF
        """
        subquery_ranges = []
        table_index = None
        alias_index = None
        for subquery_index in subquery_indices:
            open_paren_count = 0
            end_index = subquery_index
            for i in range(subquery_index, len(query_parts)):

                part = query_parts[i]

                if part.upper() == "FROM":
                    print("table_index: ", i)
                    table_index = i + 1
                # in case of nesting, make sure we're out of the subquery before detecting the alias
                if "(" in part:
                    open_paren_count += part.count("(")
                if ")" in part:
                    open_paren_count -= part.count(")")
                    if open_paren_count == 0:
                        end_index = i
                        alias_index = i + 1
                        break

            subquery_ranges.append(
                (subquery_index, end_index, table_index, alias_index)
            )
        print("subquery_ranges: ", subquery_ranges)

        tables_aliases = []
        for subquery_range in reversed(subquery_ranges):
            table = query_parts[subquery_range[2]]

            # Replace subquery with table name and alias
            replaced_query_parts = (
                query_parts[: subquery_range[0]]
                + [table]
                + query_parts[subquery_range[1] + 1 :]
            )
            # Update query parts for the next iteration
            query_parts = replaced_query_parts

        # Join the query parts to form the modified query
        modified_query = " ".join(query_parts)
        print("Modified Query:", modified_query)
        print("tables_aliases: ", tables_aliases)

        return extract_from_clause_no_subquery(modified_query)

    def tables_aliases(query):

        query_parts = query.split(" ")
        from_index = query_parts.index("FROM")
        query_parts = query_parts[from_index:]

        print("query_parts: ", query_parts)

        subquery_indices = []
        # check for subqueries
        for index, part in enumerate(query_parts):
            if re.search(r"\bSELECT\b", part):
                print(
                    f"subquery found @ index {index}",
                )
                subquery_indices.append(index)
        print("subquery_indices: ", subquery_indices)
        # if there are subqueries
        if len(subquery_indices) > 0:
            data = extract_from_clause_with_subquery(
                query, query_parts, subquery_indices
            )
        # if no subqueries
        else:
            data = extract_from_clause_no_subquery(query)

        return data

    for query in queries:
        print()
        from_tables = tables_aliases(query)
        print("tables:", from_tables)
        print()


if __name__ == "__main__":
    main()
