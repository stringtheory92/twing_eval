import re
import pandas as pd

from refined import main as revert_aliases
from tables_columns import main as tables_columns_table


def queries(data):
    return data["query_text"]


def generate_join_details(data):

    return data


def main():
    refined_table = revert_aliases()
    tables_columns_data = tables_columns_table()
    join_details_table = generate_join_details(refined_table)

    # Your SQL queries
    queries = [
        # "SELECT ymd, region_id, SUM(spend) AS total_spend, SUM(cost) AS total_cost FROM agg_monthly GROUP BY ymd, region_id;",
        # "SELECT * FROM public.agg_hourly WHERE ymd = '2024-02-16' AND hour = 9;",
        # "SELECT * FROM public.agg_daily srt LEFT JOIN (SELECT id FROM public.brand WHERE name = 'A') A ON srt.brand_id = A.id LEFT JOIN (SELECT id FROM public.brand WHERE name = 'B') B ON srt.brand_id = B.id LEFT JOIN (SELECT id FROM public.brand WHERE name = 'C') C ON srt.brand_id = C.id GROUP BY srt.ymd;",
        """SELECT brt.ymd, r.name AS region_name, b.name AS brand_name,
              os.name AS operating_system,
              br.name AS browser_name,
              SUM(brt.impressions) AS total_impressions,
              SUM(brt.clicks) AS total_clicks,
              AVG(brt.views) AS avg_views,
              SUM(brt.conversions) AS total_conversions,
              SUM(brt.spend) AS total_spend,
              (SUM(brt.clicks) / NULLIF(SUM(brt.impressions), 0)) * 100 AS click_through_rate_percentage
          FROM public.agg_daily brt
          JOIN public.region r ON brt.region_id = r.id
          JOIN public.brand b ON brt.brand_id = b.id
          JOIN public.browser br ON brt.browser_id = br.id
          JOIN public.operating_system os ON brt.os_id = os.id
          GROUP BY brt.ymd, r.name, b.name, os.name, br.name
          ORDER BY brt.ymd, total_impressions DESC;""",
    ]

    def extract_from_clause(query):
        from_pattern = re.compile(
            r"FROM\s+(.+?)(?=\s+(?:GROUP\s+BY|HAVING|ORDER\s+BY|WHERE|$))",
            re.IGNORECASE | re.DOTALL,
        )
        return from_pattern.search(query)

    def tables_aliases(query):
        # matches = from_pattern.search(query)
        matches = extract_from_clause(query)
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

    for query in queries:
        print()
        from_clause = extract_from_clause(query)
        from_tables = tables_aliases(query)
        if from_clause:
            print("first table: ", from_tables.iloc[0])
            print("tables:", from_tables)
            print()
        else:
            print("No FROM clause found in query:", query)


if __name__ == "__main__":
    main()
